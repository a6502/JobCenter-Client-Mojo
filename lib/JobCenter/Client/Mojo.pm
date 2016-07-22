package JobCenter::Client::Mojo;
use Mojo::Base -base;

our $VERSION = '0.01'; # VERSION

#
# Mojo's default reactor uses EV, and EV does not play nice with signals
# without some handholding. We either can try to detect EV and do the
# handholding, or try to prevent Mojo using EV.
#
BEGIN {
	$ENV{'MOJO_REACTOR'} = 'Mojo::Reactor::Poll' unless $ENV{'MOJO_REACTOR'};
}
# more Mojolicious
use Mojo::IOLoop;
use Mojo::Log;

# standard perl
use Carp qw(croak);
use Cwd qw(realpath);
use Data::Dumper;
use File::Basename;
use FindBin;

# from cpan
use JSON::RPC2::TwoWay;
# JSON::RPC2::TwoWay depends on JSON::MaybeXS anyways, so it can be used here
# without adding another dependency
use JSON::MaybeXS qw(decode_json encode_json);
use MojoX::NetstringStream;

has [qw(
	actions address auth conn daemon debug jobs json log port rpc
	timeout tls token who
)];

sub new {
	my ($class, %args) = @_;
	my $self = $class->SUPER::new();

	my $address = $args{address} // '127.0.0.1';
	my $debug = $args{debug} // 0; # or 1?
	my $json = $args{json} // 1;
	my $log = $args{log} // Mojo::Log->new(level => ($debug) ? 'debug' : 'info');
	my $method = $args{method} // 'password';
	my $port = $args{port} // 6522;
	my $timeout = $args{timeout} // 60;
	my $tls = $args{tls} // 0;
	my $token = $args{token} or croak 'no token?';
	my $who = $args{who} or croak 'no who?';

	my $rpc = JSON::RPC2::TwoWay->new(debug => $debug) or croak 'no rpc?';
	$rpc->register('greetings', sub { $self->rpc_greetings(@_) }, notification => 1);
	$rpc->register('job_done', sub { $self->rpc_job_done(@_) }, notification => 1);
	$rpc->register('ping', sub { $self->rpc_ping(@_) });
	$rpc->register('task_ready', sub { $self->rpc_task_ready(@_) }, notification => 1);

	my $clientid = Mojo::IOLoop->client({
		address => $address,
		port => $port,
		tls => $tls,
	} => sub {
		my ($loop, $err, $stream) = @_;
		my $ns = MojoX::NetstringStream->new(stream => $stream);
		my $conn = $rpc->newconnection(
			owner => $self,
			write => sub { $ns->write(@_) },
		);
		$self->{conn} = $conn;
		$ns->on(chunk => sub {
			my ($ns2, $chunk) = @_;
			#say 'got chunk: ', $chunk;
			my @err = $conn->handle($chunk);
			$log->debug('chunk handler: ' . join(' ', grep defined, @err)) if @err;
			$ns->close if $err[0];
		});
		$ns->on(close => sub {
			$conn->close;
			say 'aaarghel?';
			#exit(1);
		});
	});

	$self->{actions} = {};
	$self->{address} = $address;
	$self->{clientid} = $clientid;
	$self->{debug} = $args{debug} // 1;
	$self->{jobs} = {};
	$self->{json} = $json;
	$self->{log} = $log;
	$self->{port} = $port;
	$self->{rpc} = $rpc;
	$self->{timeout} = $timeout;
	$self->{tls} = $tls;
	$self->{token} = $token;
	$self->{who} = $who;

	# handle timeout?
	my $tmr = Mojo::IOLoop->timer($timeout => sub {
		my $loop = shift;
		$log->error('timeout wating for greeting');
		$loop->remove($clientid);
		$self->{auth} = 0;
	});

	$self->log->debug('starting handshake');
	Mojo::IOLoop->one_tick while !defined $self->{auth};
	$self->log->debug('done with handhake?');

	Mojo::IOLoop->remove($tmr);
	return $self if $self->{auth};
	return;
}

sub rpc_greetings {
	my ($self, $c, $i) = @_;
	Mojo::IOLoop->delay->steps(
		sub {
			my $d = shift;
			$self->log->info('got greeting from ' . $i->{who});
			$c->call('hello', {who => $self->who, method => 'password', token => $self->token}, $d->begin(0));
		},
		sub {
			my ($d, $e, $r, $w) = @_;
			#say 'hello returned: ', Dumper(\@_);
			die "hello returned error $e->{message} ($e->{code})" if $e;
			die 'no results from hello?' unless $r;
			($r, $w) = @$r;
			if ($r) {
				$self->log->info("hello returned: $r, $w");
				$self->{auth} = 1;
			} else {
				$self->log->error('hello failed' . ($w // ''));
				$self->{auth} = 0; # defined but false
			}
		}
	)->catch(sub {
		my ($delay, $err) = @_;
		$self->log->error('something went wrong in handshake: ' . $err);
		$self->{auth} = '';
	});
}

sub rpc_job_done {
	my ($self, $conn, $i) = @_;
	my $job_id = $i->{job_id};
	my $outargs = $i->{outargs};
	my $outargsj = encode_json($outargs);
	$outargs = $outargsj if $self->{json};
	my $callcb = delete $self->{jobs}->{$job_id};
	if ($callcb) {
		$self->log->debug("got job_done: for job_id  $job_id result: $outargsj");
		local $@;
		eval {
			$callcb->($job_id, $outargs);
		};
		$self->log->info("got $@ calling callback");
	} else {
		$self->log->debug("got job_done for unknown job $job_id result:  $outargsj");
	}
}

sub call {
	my ($self, %args) = @_;
	my ($done, $job_id, $outargs);
	$args{cb} = sub {
		($job_id, $outargs) = @_;
		$done++;
	};
	$job_id = $self->call_nb(%args);
	return unless $job_id;

	Mojo::IOLoop->one_tick while !$done;

	return $job_id, $outargs;
}

sub call_nb {
	my ($self, %args) = @_;
	my $wfname = $args{wfname} or die 'no workflowname?';
	my $vtag = $args{vtag};
	my $inargs = $args{inargs} // '{}';
	my $callcb = $args{cb} // die 'no callback?';
	my $inargsj;

	if ($self->{json}) {
		$inargsj = $inargs;
		$inargs = decode_json($inargs);
		croak 'inargs is not a json object' unless ref $inargs eq 'HASH';
	} else {
		croak 'inargs should be a hashref' unless ref $inargs eq 'HASH';
		# test encoding
		$inargsj = encode_json($inargs);
	}

	$self->log->debug("calling $wfname with '$inargsj'" . (($vtag) ? " (vtag $vtag)" : ''));
	my $job_id;

	my $delay = Mojo::IOLoop->delay->steps(
		sub {
			my $d = shift;
			$self->conn->call('create_job', { wfname => $wfname, vtag => $vtag, inargs => $inargs }, $d->begin(0));
		},
		sub {
		 	my ($d, $e, $r) = @_;
		 	if ($e) {
			 	$self->log->error("create_job returned error: $e->{message} ($e->{code})");
			 	return;
			}
			$job_id = $r or return;
			$self->log->debug("create_job returned job_id: $job_id");
			$self->jobs->{$job_id} = $callcb;
		}
	)->catch(sub {
		my ($delay, $err) = @_;
		$self->log->error("Something went wrong in call_nb: $err");
	});
	$delay->wait;

	return $job_id;
}

sub work {
	my ($self) = @_;
	if ($self->daemon) {
		daemonize();
	}

	$self->log->debug('JobCenter::Client::Mojo starting work');
	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
	$self->log->debug('JobCenter::Client::Mojo done?');

	return 0;
}

sub announce {
	my ($self, %args) = @_;
	my $actionname = $args{actionname} or croak 'no actionname?';
	my $cb = $args{cb} or croak 'no cb?';
	my $async = $args{async} // 0;
	my $slots = $args{slots} // 1;
	
	croak "already have action $actionname" if $self->actions->{$actionname};
	
	my $err;
	Mojo::IOLoop->delay->steps(
	sub {
		my $d = shift;
		# fixme: check results?
		$self->conn->call('announce', { actionname => $actionname, slots => $slots }, $d->begin(0));
	},
	sub {
		#say 'call returned: ', Dumper(\@_);
		my ($d, $e, $r) = @_;
		if ($e) {
			$self->log->debug("announce got error $e");
			$err = $e;
		}
		my ($res, $msg) = @$r;
		$self->log->debug("announce got res: $res msg: $msg");
		$self->actions->{$actionname} = { cb => $cb, async => $async, slots => $slots } if $res;
		$err = $msg unless $res;
	})->catch(sub {
		my $d;
		($d, $err) = @_;
		$self->log->debug("something went wrong with announce: $err");
	})->wait();

	return $err;
}

sub rpc_ping {
	my ($self, $c, $i, $rpccb) = @_;
	#my $tmr = Mojo::IOLoop->timer(3 => sub { $rpccb->('pong!'); } );
	#return;
	return 'pong!';
}

sub rpc_task_ready {
	#say 'got task_ready: ', Dumper(\@_);
	my ($self, $c, $i) = @_;
	my $actionname = $i->{actionname};
	my $job_id = $i->{job_id};
	my $action = $self->actions->{$actionname};
	unless ($action) {
		$self->log->info("got task_ready for unknown action $actionname");
		return;
	}

	$self->log->debug("got task_ready for $actionname job_id $job_id calling get_task");
	Mojo::IOLoop->delay->steps(sub {
		my $d = shift;
		$c->call('get_task', {actionname => $actionname, job_id => $job_id}, $d->begin(0));
	},
	sub {
		my ($d, $e, $r) = @_;
		#say 'get_task returned: ', Dumper(\@_);
		if ($e) {
			$self->log->debug("got $e->{message} ($e->{code}) calling get_task");
		}
		unless ($r) {
			self->log->debug('no task for get_task');
			return;
		}
		my ($cookie, $inargs) = @$r;
		unless ($cookie) {
			self->log->debug('aaah? no cookie? (get_task)');
			return;
		}
		local $@;
		if ($action->{async}) {
			eval {
				$action->{cb}->($job_id, $inargs, sub {
					$c->notify('task_done', { cookie => $cookie, outargs => $_[0] });
				});
			};
			$c->notify('task_done', { cookie => $cookie, outargs => { error => $@ } }) if $@;
		} else { 
			my $outargs = eval { $action->{cb}->($job_id, $inargs) };
			$outargs = { error => $@ } if $@;
			$c->notify('task_done', { cookie => $cookie, outargs => $outargs });
		}
	});
}

1;

=encoding utf8

=head1 NAME

JobCenter::Client::Mojo - JobCenter JSON-RPC 2.0 Api client using Mojo.

=head1 SYNOPSIS

  use JobCenter::Client::Mojo;

   my $client = JobCenter::Client::Mojo->new(
     address => ...
     port => ...
     who => ...
     token => ...
   );

   my ($job_id, $outargs) = $client->call(
     wfname => 'test',
     inargs => { test => 'test' },
   );

=head1 DESCRIPTION

L<JobCenter::Client::Mojo> is a class to build a client to connect to the
JSON-RPC 2.0 Api of the L<JobCenter> workflow engine.  The client can be
used to create and inspect jobs as well as for providing 'worker' services
to the JobCenter.

=head1 METHODS

=head2 new

$client = JobCenter::Client::Mojo->new(%arguments);

Class method that returns a new JobCenter::Client::Mojo object.

Valid arguments are:

=over 4

=item - address: address of the Api.

(default: 127.0.0.1)

=item - port: port of the Api

(default 6522)

=item - tls: connect using tls

(default false)

=item - who: who to authenticate as.

(required)

=item - method: how to authenticate.

(default: password)

=item - token: token to authenticate with.

(required)

=item - debug: when true prints debugging using L<Mojo::Log>

(default: false)

=item - json: flag wether input is json or perl.

when true expects the inargs to be valid json, when false a perl hashref is
expected and json encoded.  (default true)

=item - log: L<Mojo::Log> object to use

(per default a new L<Mojo::Log> object is created)

=item - timeout: how long to wait for operations to complete

(default 60 seconds)

=back

=head2 call

($job_id, result) = $client->call(%args);

Creates a new L<JobCenter> job and waits for the results.  Throws an error
if somethings goes wrong immediately.  Errors encountered during later
processing are returned as a L<JobCenter> error object.

Valid arguments are:

=over 4

=item - wfname: name of the workflow to call (required)

=item - inargs: input arguments for the workflow (if any)

=item - vtag: version tag of the workflow to use (optional)

=back

=head2 call_nb

$job_id = $client->call_nb(%args);

Creates a new L<JobCenter> job and call the provided callback on completion
of the job.  Throws an error if somethings goes wrong immediately.  Errors
encountered during later processing are returned as a L<JobCenter> error
object to the callback.

Valid arguments are those for L<call> and:

=over 4

=item - cb: coderef to the callback to call on completion (requird)

( cb => sub { ($job_id, $outargs) = @_; ... } )

=back

=head2 announce

Announces the capability to do an action to the Api.  The provided callback
will be called when there is a task to be performed.  Returns an error when
there was a problem announcing the action.

  my $err = $client->announce(
    actionname => '...',
    cb => sub { ... },
  );
  die "could not announce $actionname?: $err" if $err;

See L<jcworker> for an example.

Valid arguments are:

=over 4

=item - actionname: name of the action

(required)

=item - cb: callback to be called for the action

(required)

=item - async: if true then the callback gets passed another callback as the
last argument that is to be called on completion of the task.

(default false)

=item - slots: the amount of tasks the worker is able to process in parallel
for this action.

(default 1)

=back

=head2 work

Starts the L<Mojo::IOLoop>.

=head1 SEE ALSO

L<Jobcenter>, L<jcclient>, L<jcworker>,
L<Mojo::IOLoop>.

=cut

1;
