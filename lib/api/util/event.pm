#
# api::util::event.pm
#
# Copyright (c) 2011 Marko Dinic <marko@yu.net>. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

package api::util::event;

##########################################################################################

use strict;
use warnings;

##########################################################################################

use Fcntl;
use IO::Poll;
use Time::HiRes qw(time alarm sleep);
use POSIX qw(:signal_h :sys_wait_h);

##########################################################################################

use api::util::table;
use api::util::lru;

##########################################################################################

#
# Module constructor
#
#  This function is called in order to create
#  API module instance.
#
#   Input:	1. class name (passed implicitly)
#
#   Output:	1. api::util::event object reference
#
sub new($) {
    my $class = shift;

    my $read_poll = IO::Poll->new();
    return undef unless defined($read_poll);

    my $write_poll = IO::Poll->new();
    return undef unless defined($write_poll);

    my $timer = api::util::lru->new(1);
    return undef unless defined($timer);

    my $expire = api::util::lru->new(1);
    return undef unless defined($expire);

    my $delay = api::util::lru->new(1);
    return undef unless defined($delay);

    my $cont = api::util::table->new();
    return undef unless defined($cont);

    # Poll must be monitoring at least one handle,
    # so we register something that shouldn't happen
    # unless we do it ourselves - STDOUT close
    $read_poll->mask(STDOUT => POLLHUP);

    my $self = {
	'epoch' => time(),
	'rpoll' => $read_poll,
	'wpoll' => $write_poll,
	'timer' => $timer,
	'expire' => $expire,
	'delay' => $delay,
	'cont' => $cont,
	'read' => {},
	'write' => {}
    };

    return bless($self, $class);
}
#
# Register continuous event (one that is always in effect)
#
#  This method registers a continuous event that is active
#  at all times. Event poll() method always returns event
#  handlers for this type of event.
#
#  Event can be bound by any or all of the following:
#   - maximum number of repetitions (iterations),
#   - amount of time elapsed since its creation
#   - specific expiry time given as UNIX timestamp.
#
#  If multiple limits are defined, event is terminated by whichever
#  limit is reached first.
#
#   Input:	1. self object reference (passed implicitly)
#		2. parameter hash:
#
#		  'handler' => coderef to the event handler callback (mandatory)
#		  'args' => arrayref to the callback arguments (optional)
#		  'delay' => start up (first trigger) delay in seconds (optional)
#		  'attempts' => number of attempts to complete successfully (optional)
#		  'timeout' => single attempt timeout (optional)
#		  'on_timeout' => callback invoked when no more attempts remain (optional)
#		  'limit' => maximum number of iterations (optional)
#		  'on_limit' => repetition limit handler callback (optional)
#		  'expire_in' => maximum lifetime of the event (optional)
#		  'expire_at' => specific termination time of the event (optional)
#		  'on_expiry' => event expiration handler callback (optional)
#		  'event' => ref to a hash to be used as the event struct (optional)
#
#   Output:	1. continuous event hashref, if successful
#		   undef, if failed
#
sub continuous_event($%) {
    my $self = shift;
    my $param = {@_};

    # Event handler callback must be coderef
    return undef unless(defined($param->{'handler'}) && ref($param->{'handler'}) eq 'CODE');

    # If caller passes a reference to an existing
    # hash, that hash will be used as the event
    # structure. Otherwise, new anonymous hash
    # will be used.
    my $event = defined($param->{'event'}) &&
		ref($param->{'event'}) eq 'HASH' ?
			$param->{'event'}:{};
    # This is our event including its handler.
    %{$event} = (
	'type'		=> 'continuous',
	'handler'	=> $param->{'handler'},
	'args'		=> (ref($param->{'args'}) eq 'ARRAY') ?
				$param->{'args'}:[],
	'delay'		=> (defined($param->{'delay'}) && $param->{'delay'} > 0) ?
				$param->{'delay'}:undef,
	'attempts'	=> (defined($param->{'attempts'}) && int($param->{'attempts'}) > 0) ?
				int($param->{'attempts'}):undef,
	'timeout'	=> (defined($param->{'timeout'}) && $param->{'timeout'} > 0) ?
				$param->{'timeout'}:undef,
	'on_timeout'	=> (ref($param->{'on_timeout'}) eq 'CODE') ?
				$param->{'on_timeout'}:undef,
	'limit'		=> (defined($param->{'limit'}) && int($param->{'limit'}) > 0) ?
				int($param->{'limit'}):undef,
	'on_limit'	=> (ref($param->{'on_limit'}) eq 'CODE') ?
				$param->{'on_limit'}:undef,
	'expire_in'	=> (defined($param->{'expire_in'}) && $param->{'expire_in'} > 0) ?
				$param->{'expire_in'}:undef,
	'expire_at'	=> (defined($param->{'expire_at'}) && $param->{'expire_at'} > time()) ?
				$param->{'expire_at'}:undef,
	'on_expiry'	=> (ref($param->{'on_expiry'}) eq 'CODE') ?
				$param->{'on_expiry'}:undef
    );

    # Event limit callback wrapper
    if(defined($event->{'on_limit'})) {
	$event->{'_on_limit'} = sub {
	    return $self->invoke_handler('handler'	=> $event->{'on_limit'},
					 'args'		=> [ $event, @{$event->{'args'}}, @_ ],
					 'attempts'	=> $event->{'attempts'},
					 'timeout'	=> $event->{'timeout'},
					 'on_timeout'	=> $event->{'on_timeout'});
	};
    }

    # Event expiry callback wrapper
    if(defined($event->{'on_expiry'})) {
	$event->{'_on_expiry'} = sub {
	    return $self->invoke_handler('handler'	=> $event->{'on_expiry'},
					 'args'		=> [ $event, @{$event->{'args'}}, @_ ],
					 'attempts'	=> $event->{'attempts'},
					 'timeout'	=> $event->{'timeout'},
					 'on_timeout'	=> $event->{'on_timeout'});
	};
    }

    # Event handler wrapper
    $event->{'_handler'} = sub {
	return $self->invoke_handler('handler'		=> $event->{'handler'},
				     'args'		=> [ $event, @{$event->{'args'}}, @_ ],
				     'attempts'		=> $event->{'attempts'},
				     'timeout'		=> $event->{'timeout'},
				     'on_timeout'	=> $event->{'on_timeout'});
    };

    # Turn absolute expiration timestamp into
    # interval relative to this very moment.
    my $until_interval = defined($event->{'expire_at'}) ?
				($event->{'expire_at'} - time()):undef;

    # Expiry interval for this event will be either
    #  - a lifetime limit (in seconds), or
    #  - a specific time (given as UNIX timestamp),
    # whichever comes first
    my $expiry_interval = defined($event->{'expire_in'}) ?
			    ((defined($until_interval) && $until_interval < $event->{'expire_in'}) ?
				$until_interval:$event->{'expire_in'}):$until_interval;

    # If any time limit is imposed ...
    if(defined($expiry_interval)) {
	# ... schedule event expiration
	$self->{'expire'}->insert($event, $expiry_interval);
    }

    # If startup delay was specified ...
    if(defined($event->{'delay'})) {
        # .. schedule event to begin in 'delay' seconds
	$self->{'delay'}->insert($event, $event->{'delay'});
    # Otherwise, set event to begin immediately
    } else {
	# ... by adding it to the table
	# of continuous events
	$self->{'cont'}->add($event);
    }

    return $event;
}
#
# Register timer event
#
#  This method registers a timer event. Each time interval expires
#  (timer ticks), event handler callback function is invoked.
#
#  Event can be bound by any or all of the following:
#   - maximum number of repetitions (ticks),
#   - amount of time elapsed since its creation
#   - specific expiry time given as UNIX timestamp.
#
#  If multiple limits are defined, event is terminated by whichever
#  limit is reached first.
#
#   Input:	1. self object reference (passed implicitly)
#		2. parameter hash:
#
#		  'interval' => tick interval of the event (mandatory)
#		  'handler' => coderef to the event handler callback (mandatory)
#		  'args' => arrayref to the callback arguments (optional)
#		  'delay' => start up (first tick) delay in seconds (optional)
#		  'attempts' => number of attempts to complete successfully (optional)
#		  'timeout' => single attempt timeout (optional)
#		  'on_timeout' => callback invoked when no more attempts remain (optional)
#		  'limit' => maximum number of ticks (optional)
#		  'on_limit' => repetition limit handler callback (optional)
#		  'expire_in' => maximum lifetime of the event (optional)
#		  'expire_at' => specific termination time of the event (optional)
#		  'event' => ref to a hash to be used as the event struct (optional)
#
#   Output:	1. timer event hashref, if successful
#		   undef, if failed
#
sub timer_event($%) {
    my $self = shift;
    my $param = {@_};

    my $time = time();

    # Callback must be coderef
    return undef unless(defined($param->{'handler'}) && ref($param->{'handler'}) eq 'CODE' &&
			defined($param->{'interval'}) && $param->{'interval'} > 0);

    # If caller passes a reference to an existing
    # hash, that hash will be used as the event
    # structure. Otherwise, new anonymous hash
    # will be used.
    my $event = defined($param->{'event'}) &&
		ref($param->{'event'}) eq 'HASH' ?
			$param->{'event'}:{};
    # This is our event including its handler.
    %{$event} = (
	'type'		=> 'timer',
	'interval'	=> $param->{'interval'},
	'handler'	=> $param->{'handler'},
	'args'		=> (ref($param->{'args'}) eq 'ARRAY') ?
				$param->{'args'}:[],
	'attempts'	=> (defined($param->{'attempts'}) && int($param->{'attempts'}) > 0) ?
				int($param->{'attempts'}):undef,
	'timeout'	=> (defined($param->{'timeout'}) && $param->{'timeout'} > 0) ?
				$param->{'timeout'}:undef,
	'on_timeout'	=> (ref($param->{'on_timeout'}) eq 'CODE') ?
				$param->{'on_timeout'}:undef,
	'limit'		=> (defined($param->{'limit'}) && int($param->{'limit'}) > 0) ?
				int($param->{'limit'}):undef,
	'on_limit'	=> (ref($param->{'on_limit'}) eq 'CODE') ?
				$param->{'on_limit'}:undef,
	'expire_in'	=> (defined($param->{'expire_in'}) && $param->{'expire_in'} > 0) ?
				$param->{'expire_in'}:undef,
	'expire_at'	=> (defined($param->{'expire_at'}) && $param->{'expire_at'} > $time) ?
				$param->{'expire_at'}:undef,
	'on_expiry'	=> (ref($param->{'on_expiry'}) eq 'CODE') ?
				$param->{'on_expiry'}:undef
    );

    # Turn relative expiration interval into
    # absolute expiration UNIX timestamp.
    my $expire_timestamp = defined($event->{'expire_in'}) ?
				($time + $event->{'expire_in'}):undef;

    # Expiry interval for this event will be either
    #  - a lifetime limit (in seconds), or
    #  - a specific time (given as UNIX timestamp),
    # whichever comes first
    $event->{'_expire_time'} = defined($event->{'expire_at'}) ?
					((defined($expire_timestamp) && $expire_timestamp < $event->{'expire_at'}) ?
					    $expire_timestamp:$event->{'expire_at'}):$expire_timestamp;

    # Event limit callback wrapper
    if(defined($event->{'on_limit'})) {
	$event->{'_on_limit'} = sub {
	    return $self->invoke_handler('handler'	=> $event->{'on_limit'},
					 'args'	=> [ $event, @{$event->{'args'}}, @_ ],
					 'attempts'	=> $event->{'attempts'},
					 'timeout'	=> $event->{'timeout'},
					 'on_timeout'	=> $event->{'on_timeout'});
	};
    }

    # Event expiry callback wrapper
    if(defined($event->{'on_expiry'})) {
	$event->{'_on_expiry'} = sub {
	    return $self->invoke_handler('handler'	=> $event->{'on_expiry'},
					 'args'		=> [ $event, @{$event->{'args'}}, @_ ],
					 'attempts'	=> $event->{'attempts'},
					 'timeout'	=> $event->{'timeout'},
					 'on_timeout'	=> $event->{'on_timeout'});
	};
    }

    # Event handler wrapper
    $event->{'_handler'} = sub {
	return $self->invoke_handler('handler'		=> $event->{'handler'},
				     'args'		=> [ $event, @{$event->{'args'}}, @_ ],
				     'attempts'		=> $event->{'attempts'},
				     'timeout'		=> $event->{'timeout'},
				     'on_timeout'	=> $event->{'on_timeout'});
    };

    # Schedule periodic event in LRU
    $self->{'timer'}->insert($event,
			     (defined($param->{'delay'}) &&
			      $param->{'delay'} > 0) ?
				$param->{'delay'}:0.00001);

    return $event;
}
#
# Register I/O event
#
#  This method registers an I/O event on a file/pipe/socket/etc.
#  On event, callback function is invoked.
#
#  Event can be bound by any or all of the following:
#   - maximum number of event triggers,
#   - life time since its creation
#   - specific expiry time given as UNIX timestamp.
#
#  If multiple limits are defined, event is terminated by whichever
#  limit is reached first.
#
#   Input:	1. self object reference (passed implicitly)
#		2. parameter hash:
#
#		  'file' => file handle, socket, etc (mandatory)
#		  'op' => I/O operation ('r' - read, 'w' - write)
#		  'handler' => coderef to the event handler callback (mandatory)
#		  'args' => arrayref to the callback arguments (optional)
#		  'delay' => trigger delay in seconds (optional)
#		  'attempts' => number of attempts to complete successfully (optional)
#		  'timeout' => single attempt timeout (optional)
#		  'on_timeout' => callback invoked when no more attempts remain (optional)
#		  'limit' => maximum number of occurrences (optional)
#		  'on_limit' => repetition limit handler callback (optional)
#		  'expire_in' => maximum lifetime of the event (optional)
#		  'expire_at' => specific termination time of the event (optional)
#		  'event' => ref to a hash to be used as the event struct (optional)
#
#   Output:	1. file handle, if successful
#		   undef, if failed
#
sub io_event($%) {
    my $self = shift;
    my $param = {@_};

    # Get file descriptor for file
    my $fd = fileno($param->{'file'});

    # File descriptor must be valid,
    # operation must be 'r'ead or 'w'rite,
    # callback must be coderef
    return undef unless(defined($param->{'handler'}) && ref($param->{'handler'}) eq 'CODE' &&
			defined($param->{'op'}) && $param->{'op'} =~ /^r|w$/ &&
			defined($fd));

    # If caller passes a reference to an existing
    # hash, that hash will be used as the event
    # structure. Otherwise, new anonymous hash
    # will be used.
    my $event = defined($param->{'event'}) &&
		ref($param->{'event'}) eq 'HASH' ?
			$param->{'event'}:{};
    # This is our event including its handler.
    %{$event} = (
	'type'		=> 'io',
	'file'		=> $param->{'file'},
	'op'		=> $param->{'op'},
	'handler'	=> $param->{'handler'},
	'args'		=> (ref($param->{'args'}) eq 'ARRAY') ?
				$param->{'args'}:[],
	'delay'		=> (defined($param->{'delay'}) && $param->{'delay'} > 0) ?
				$param->{'delay'}:undef,
	'attempts'	=> (defined($param->{'attempts'}) && int($param->{'attempts'}) > 0) ?
				int($param->{'attempts'}):undef,
	'timeout'	=> (defined($param->{'timeout'}) && $param->{'timeout'} > 0) ?
				$param->{'timeout'}:undef,
	'on_timeout'	=> (ref($param->{'on_timeout'}) eq 'CODE') ?
				$param->{'on_timeout'}:undef,
	'limit'		=> (defined($param->{'limit'}) && int($param->{'limit'}) > 0) ?
				int($param->{'limit'}):undef,
	'on_limit'	=> (ref($param->{'on_limit'}) eq 'CODE') ?
				$param->{'on_limit'}:undef,
	'expire_in'	=> (defined($param->{'expire_in'}) && $param->{'expire_in'} > 0) ?
				$param->{'expire_in'}:undef,
	'expire_at'	=> (defined($param->{'expire_at'}) && $param->{'expire_at'} > time()) ?
				$param->{'expire_at'}:undef,
	'on_expiry'	=> (ref($param->{'on_expiry'}) eq 'CODE') ?
				$param->{'on_expiry'}:undef
    );

    # Event limit callback wrapper
    if(defined($event->{'on_limit'})) {
	$event->{'_on_limit'} = sub {
	    return $self->invoke_handler('handler'	=> $event->{'on_limit'},
					 'args'		=> [ $event->{'file'}, @{$event->{'args'}}, @_ ],
					 'attempts'	=> $event->{'attempts'},
					 'timeout'	=> $event->{'timeout'},
					 'on_timeout'	=> $event->{'on_timeout'});
	};
    }

    # Event expiry callback wrapper
    if(defined($event->{'on_expiry'})) {
	$event->{'_on_expiry'} = sub {
	    return $self->invoke_handler('handler'	=> $event->{'on_expiry'},
					 'args'		=> [ $event->{'file'}, @{$event->{'args'}}, @_ ],
					 'attempts'	=> $event->{'attempts'},
					 'timeout'	=> $event->{'timeout'},
					 'on_timeout'	=> $event->{'on_timeout'});
	};
    }

    # Event handler wrapper
    $event->{'_handler'} = sub {
	return $self->invoke_handler('handler'		=> $event->{'handler'},
				     'args'		=> [ $event->{'file'}, @{$event->{'args'}}, @_ ],
				     'attempts'		=> $event->{'attempts'},
				     'timeout'		=> $event->{'timeout'},
				     'on_timeout'	=> $event->{'on_timeout'});
    };

    # Turn absolute expiration timestamp into
    # interval relative to this very moment.
    my $until_interval = defined($event->{'expire_at'}) ?
				$event->{'expire_at'} - time():undef;

    # Expiry interval for this event will be either
    #  - a lifetime limit (in seconds), or
    #  - a specific time (given as UNIX timestamp),
    # whichever comes first
    my $expiry_interval = defined($event->{'expire_in'}) ?
			    ((defined($until_interval) && $until_interval < $event->{'expire_in'}) ?
				$until_interval:$event->{'expire_in'}):$until_interval;

    # If any time limit is imposed ...
    if(defined($expiry_interval)) {
	# ... schedule event expiration in LRU
	$self->{'expire'}->insert($event, $expiry_interval);
    }

    # Monitor file descriptor for read events ?
    if($event->{'op'} eq 'r') {
	# Setup poll() for read events
	$self->{'rpoll'}->mask($event->{'file'} => POLLIN);
	# Put newly created event into the list
	$self->{'read'}{$fd} = $event;
    # Monitor file descriptor for write events ?
    } elsif($event->{'op'} eq 'w') {
	# Setup poll() for write events
	$self->{'wpoll'}->mask($event->{'file'} => POLLOUT);
	# Put newly created event into the list
	$self->{'write'}{$fd} = $event;
    }

    return $event->{'file'};
}
#
# Unregister continuous event
#
#  This method removes continuous event from event monitor.
#
#   Input:	1. self object reference (passed implicitly)
#		2. continuous event hashref
#
#   Output:	none
#
sub remove_continuous_event($$) {
    my ($self, $event) = @_;

    return unless(defined($event) &&
		  ref($event) eq 'HASH');

    # Remove event from expiry timer
    $self->{'expire'}->remove($event);
    # Remove event from delay timer
    $self->{'delay'}->remove($event);
    # Remove event from the table
    $self->{'cont'}->remove($event);
}
#
# Unregister timer event
#
#  This method removes periodic or scheduled event from
#  event monitor.
#
#   Input:	1. self object reference (passed implicitly)
#		2. timer event hashref
#
#   Output:	none
#
sub remove_timer_event($$) {
    my ($self, $event) = @_;

    return unless(defined($event) &&
		  ref($event) eq 'HASH');

    # Remove timer event from timer
    $self->{'timer'}->remove($event);
}
#
# Remove I/O event
#
#  This method removes file read event from event monitor.
#
#   Input:	1. self object reference (passed implicitly)
#		2. file/pipe/socket/etc ...
#		3. I/O - operation ('r'-read, 'w'-write, 'rw'-read/write)
#
#   Output:	none
#
sub remove_io_event($$;$) {
    my $self = shift;
    my $file = shift;
    my $op = shift;

    my $fd = fileno($file);
    return unless defined($fd);

    # Destroy I/O event completely ?
    if(!defined($op) || $op eq 'rw') {
	# Remove file descriptor from poll()s
	$self->{'rpoll'}->remove($file);
	$self->{'wpoll'}->remove($file);
	# Remove all expiration timers
	$self->{'expire'}->remove($self->{'read'}{$fd});
	$self->{'expire'}->remove($self->{'write'}{$fd});
	# Remove all delay timers
	$self->{'delay'}->remove($self->{'read'}{$fd});
	$self->{'delay'}->remove($self->{'write'}{$fd});
	# Remove all I/O events
	delete $self->{'read'}{$fd};
	delete $self->{'write'}{$fd};
    # Stop monitoring file for read events ?
    } elsif($op eq 'r') {
	# Remove read expiration timer, if any
	$self->{'expire'}->remove($self->{'read'}{$fd});
	# Remove read delay timer, if any
	$self->{'delay'}->remove($self->{'read'}{$fd});
	# Remove read event
	delete $self->{'read'}{$fd};
	# Remove file from read poll
	$self->{'rpoll'}->remove($file);
    # Stop monitoring file for write events ?
    } elsif($op eq 'w') {
	# Remove write expiration timer, if any
	$self->{'expire'}->remove($self->{'write'}{$fd});
	# Remove write delay timer, if any
	$self->{'delay'}->remove($self->{'write'}{$fd});
	# Remove write event
	delete $self->{'write'}{$fd};
	# Remove file from write poll
	$self->{'wpoll'}->remove($file);
    }
}
#
# Modify continuous event
#
#  This method modifies already registered continuous event.
#
#   Input:	1. self object reference (passed implicitly)
#		2. continuous event hashref
#		3. parameter hash (all parameters are optional):
#
#		  'handler' => coderef to the event handler callback
#		  'args' => arrayref to the callback arguments
#		  'delay' => start up (first trigger) delay in seconds
#		  'attempts' => number of attempts to complete successfully
#		  'timeout' => single attempt timeout
#		  'on_timeout' => callback invoked when no more attempts remain
#		  'limit' => maximum number of iterations
#		  'on_limit' => repetition limit handler callback
#		  'expire_in' => maximum lifetime of the event
#		  'expire_at' => specific termination time of the event
#		  'on_expiry' => event expiration handler callback
#
#   Output:	1. continuous event hashref, if successful
#		   undef, if failed
#
sub modify_continuous_event($$;%) {
    my $self = shift;
    my $event = shift;
    my %param = (@_);

    # Input params must be sane
    return undef unless(defined($event) && ref($event) eq 'HASH' &&
			keys %param > 0);

    # Cannot modify event type
    # or event base parameters
    delete $param{'type'};

    # If event has expiration time,
    # we need to reschedule it
    $event->{'expire_in'} = $self->{'expire'}->due($event);

    # Remaining delay of event that was
    # already triggered (if any)
    $event->{'delay'} = $self->{'delay'}->due($event);

    # Merge new parameters with existing event
    foreach my $key (keys %param) {
	# Replace event params with new ones
	$event->{$key} = $param{$key};
    }

    # Use existing event as parameter hash
    # for new event that we are about to
    # register in place of current one.
    $event->{'event'} = $event;

    # Remove continuous event
    $self->remove_continuous_event($event);

    # Re-register continuous event
    # with modified parameters
    return $self->continuous_event(%{$event})
}
#
# Modify timer event
#
#  This method modifies already registered timer event.
#
#   Input:	1. self object reference (passed implicitly)
#		2. timer event hashref
#		3. parameter hash (all parameters are optional):
#
#		  'interval' => tick interval of the event
#		  'handler' => coderef to the event handler callback
#		  'args' => arrayref to the callback arguments
#		  'delay' => start up (first tick) delay in seconds
#		  'attempts' => number of attempts to complete successfully
#		  'timeout' => single attempt timeout
#		  'on_timeout' => callback invoked when no more attempts remain
#		  'limit' => maximum number of ticks
#		  'on_limit' => repetition limit handler callback
#		  'expire_in' => maximum lifetime of the event
#		  'expire_at' => specific termination time of the event
#
#   Output:	1. timer event hashref, if successful
#		   undef, if failed
#
sub modify_timer_event($$;%) {
    my $self = shift;
    my $event = shift;
    my %param = (@_);

    # Input params must be sane
    return undef unless(defined($event) && ref($event) eq 'HASH' &&
			keys %param > 0);

    # Cannot modify event type
    # or event base parameters
    delete $param{'type'};

    # Remaining time until next timer tick
    $event->{'delay'} = $self->{'timer'}->due($event);

    # Merge new parameters with existing event
    foreach my $key (keys %param) {
	# Replace event params with new ones
	$event->{$key} = $param{$key};
    }

    # Use existing event as parameter hash
    # for new event that we are about to
    # register in place of current one.
    $event->{'event'} = $event;

    # Remove timer event
    $self->remove_timer_event($event);

    # Re-register timer event
    # with modified parameters
    return $self->timer_event(%{$event});
}
#
# Modify I/O event
#
#  This method modifies already registered I/O event.
#
#   Input:	1. self object reference (passed implicitly)
#		2. file handle
#		3. I/O operation ('r' - read, 'w' - write)
#		4. parameter hash (all parameters are optional):
#
#		  'handler' => coderef to the event handler callback
#		  'args' => arrayref to the callback arguments
#		  'delay' => trigger delay in seconds
#		  'attempts' => number of attempts to complete successfully
#		  'timeout' => single attempt timeout
#		  'on_timeout' => callback invoked when no more attempts remain
#		  'limit' => maximum number of occurrences
#		  'on_limit' => repetition limit handler callback
#		  'expire_in' => maximum lifetime of the event
#		  'expire_at' => specific termination time of the event
#
#   Output:	1. file handle, if successful
#		   undef, if failed
#
sub modify_io_event($$$;%) {
    my $self = shift;
    my $file = shift;
    my $op = shift;
    my %param = (@_);

    my $fd = fileno($file);
    return unless defined($fd);

    # Input params must be sane
    return undef unless(defined($param{'op'}) && $param{'op'} =~ /^r|w$/ &&
			defined($fd) && $fd > -1 &&
			keys %param > 0);

    # Get event associated with this file descriptor
    my %ops = ('r' => 'read', 'w' => 'write');
    my $event = $self->{$ops{$op}}{$fd};
    # Event must exist
    return undef unless(defined($event) && ref($event) eq 'HASH');

    # Cannot modify event type
    # or event base parameters
    delete $param{'type'};
    delete $param{'file'};
    delete $param{'op'};

    # If event has expiration time,
    # we need to reschedule it
    $event->{'expire_in'} = $self->{'expire'}->due($event);

    # Merge new parameters with existing event
    foreach my $key (keys %param) {
	# Replace event params with new ones
	$event->{$key} = $param{$key};
    }

    # Use existing event as parameter hash
    # for new event that we are about to
    # register in place of current one.
    $event->{'event'} = $event;

    # Remaining delay of event that was
    # already triggered (if any)
    my $due_in = $self->{'delay'}->due($event);

    # Remove I/O event
    $self->remove_io_event($event->{'file'}, $event->{'op'});

    # Re-register I/Or event
    # with modified parameters
    $fd = $self->io_event(%{$event});

    # If already triggered event was delayed,
    # reschedule it with remaining delay time
    if(defined($due_in) && $due_in > 0) {
	$self->{'delay'}->insert($event, $due_in);
    }

    return $file;
}
#
# Get registered event
#
#  This method returns the event hashref of the registered event
#  passed parameter belongs to. Parameter can be an event hashref,
#  file descriptor or file handle. Event hashref will be returned
#  only if event is still registered with this instance of event
#  monitor. Even if event hashref is used as first input parameter,
#  result can be undef in case passed event parameter is no longer
#  registered.
#
#   Input:	1. self object reference (passed implicitly)
#		2. event hashref, file descriptor/handle
#		3. I/O - operation ('r'-read, 'w'-write), if previous
#		   parameter was file descriptor or file handle
#
#   Output:	1. event hashref, if registered event was found,
#		   undef, if nothing was found
#
sub get_event($$;$) {
    my $self = shift;
    my $param = shift;
    my $op = shift;

    my $event;

    my %ops = ('r' => 'read', 'w' => 'write');

    # Event hashref as parameter ?
    if(ref($param) eq 'HASH') {
	if($param->{'type'} eq 'continuous') {
	    # Look for continuous event
	    $event = $self->{'cont'}->exists($param) ? $param:undef;
	} elsif($param->{'type'} eq 'timer') {
	    # Look for timer event
	    $event = $self->{'timer'}->exists($param) ? $param:undef;
	} elsif($param->{'type'} eq 'io') {
	    # Look for I/O event
	    $event = $self->{$ops{$param->{'op'}}}->exists($param) ? $param:undef;
	}
    # File handle as parameter ?
    } elsif((my $fd = fileno($param))) {
	# Get event associated with this file handle
	$event = defined($ops{$op}) ? $self->{$ops{$op}}{$fd}:undef;
    # File descriptor as parameter ?
    } else {
	# Get event associated with this file descriptor
	$event = defined($ops{$op}) ? $self->{$ops{$op}}{$param}:undef;
    }

    return $event;
}
#
# Delay next event handler trigger
#
#  This function delays next event trigger:
#
#  Continuous events are delayed immediatelly by the specified amount.
#  Timer events' next tick is rescheduled by the specified amount.
#  I/O events' next handler trigger is delayed by the specified amount.
#
#   Input:	1. self object reference (passed implicitly)
#		2. event object
#		3. delay in seconds
#
#   Output:	1. TRUE, if succeeded,
#		   FALSE, if failed
#
sub delay_event($$$) {
    my ($self, $event, $delay) = @_;

    # Input params must be sane
    return 0 unless(defined($event) && ref($event) eq 'HASH' &&
		    defined($delay) && $delay > 0);

    # Continuous event delay ?
    if($event->{'type'} eq 'continuous') {
	# Remove continuous event
	$self->remove_continuous_event($event);
	# Add delay interval
	$event->{'delay'} = $delay;
	# Use existing event hashref
	# for new event that we are
	# about to register.
	$event->{'event'} = $event;
	# Re-schedule continuous event with delay
	$self->continuous_event(%{$event})
    # timer event delay ?
    } elsif($event->{'type'} eq 'timer') {
	# Remove timer event
	$self->remove_timer_event($event);
	# Add delay interval
	$event->{'delay'} = $delay;
	# Use existing event hashref
	# for new event that we are
	# about to register.
	$event->{'event'} = $event;
	# Re-schedule timer event with delay
	$self->timer_event(%{$event});
    # I/O event delay ?
    } elsif($event->{'type'} eq 'io') {
	# To delay next event trigger,
	# simply add delay value
	$event->{'delay'} = $delay;
    }

    return 1;
}
#
# Poll for registered events
#
#  This function polls for I/O events on file handles
#  and registered timer and 'always on' (continuous)
#  events, building a list of event handlers to be called.
#
#   Input:	1. self object reference (passed implicitly)
#
#   Output:	1. array of coderefs to event handler functions
#		   for all registered events triggered between
#		   previous and current invocation of this
#		   method.
#
sub poll($) {
    my $self = shift;
    my @event_handlers = ();

    #
    # Operation of this method is non-blocking.
    #
    # I/O events are always processed immediately as
    # they happen, while other events are processed
    # in time slices.
    #
    # Time is expressed in seconds, but in floating
    # point format, so it can define intervals that
    # are just a fraction of a second.
    #

    # Current time with microsecond precision
    my $time = time();

    # If continuous events are registered, our precision
    # has to be as high as possible. Otherwise, if timer
    # events are registered, timeslice will be equal to
    # the time until the next pending synchronous event.
    #
    # If neither continuous nor timer events are scheduled,
    # default time slice will be 1 second, as the remaining
    # asynchronous (I/O) events are not affected by time
    # slices.

    my @pending_intervals = (
	($self->{'cont'}->count > 0) ? 0.0001:undef,
	$self->{'expire'}->firstdue,
	$self->{'timer'}->firstdue,
	$self->{'delay'}->firstdue,
	1
    );

    my $timeslice;

    foreach my $next (@pending_intervals) {
	# Skip empty time queues
	next unless defined $next && $next > 0;
	# Look for closest upcoming event
	unless(defined($timeslice) &&
	      $timeslice <= $next) {
	    $timeslice = $next;
	}
    }

    # Wait for I/O events using system poll()
    # as a generic hi-res wait/sleep mechanism.
    # We cannot use write poll here because,
    # unlike read operations, file handles are
    # ready for write operations most of the time,
    # so we wouldn't get any sleep which would
    # lead to high CPU utilization. Therefore, we
    # use read poll both to wait for read events
    # and to perform nano sleeps. We don't wait
    # for write events, just collect them from
    # write poll on each pass, if they are ready.
    $self->{'rpoll'}->poll($timeslice);

    # Remove all expired events, excluding timer events
    while((my $ev = $self->{'expire'}->expired())) {
	# Continuous event expiration
	if($ev->{'type'} eq 'continuous') {
	    # Simply remove continuous event
	    $self->remove_continuous_event($ev);
	# I/O event expiration
	} elsif($ev->{'type'} eq 'io') {
	    # Simply remove I/O event
	    $self->remove_io_event($ev->{'file'}, $ev->{'op'})
	}
	# Append event expiration callback coderef to
	# the list of handlers that should be called
	if(defined($ev->{'_on_expiry'})) {
	    push @event_handlers, $ev->{'_on_expiry'};
	}
    }

    # Get all read-ready file descriptors
    my @read_ready = $self->{'rpoll'}->handles(POLLIN);
    # Prepare all read-ready event handlers
    foreach my $file (@read_ready) {
	my $fd = defined($file) ? fileno($file):undef;
	next unless defined($fd);
	# If ready file handle is registered for I/O event ...
	my $ev = $self->{'read'}{$fd};
	next unless defined($ev);
	# If trigger delay is defined ...
	if(defined($ev->{'delay'}) && $ev->{'delay'} > 0) {
	    # ... schedule event handler trigger
	    $self->{'delay'}->insert($ev, $ev->{'delay'});
	    # Otherwise, trigger handler immediately
	} else {
	    # Append event handler coderef to the list
	    # of event handlers that should be called
	    push @event_handlers, $ev->{'_handler'};
	    # Is there a limit on number of event triggers ?
	    next unless defined($ev->{'limit'});
	    # Reduce trigger counter
	    $ev->{'limit'}--;
	    # If trigger counter reached zero,
	    next unless ($ev->{'limit'} < 1);
	    # Append trigger limit callback coderef to the list
	    # of event handlers that should be called
	    if(defined($ev->{'_on_limit'})) {
		push @event_handlers, $ev->{'_on_limit'};
	    }
	    # Remove the event
	    $self->remove_io_event($file, 'r');
	}
    }

    # As previosly stated, we don't wait for
    # write events, just check for ready ones
    # and collect them from write poll.
    $self->{'wpoll'}->poll(0);
    # Collect all write-ready file descriptors
    my @write_ready = $self->{'wpoll'}->handles(POLLOUT);
    # Prepare all write-ready event handlers
    foreach my $file (@write_ready) {
	my $fd = defined($file) ? fileno($file):undef;
	next unless defined($fd);
	# If ready file handle is registered for I/O event ...
	my $ev = $self->{'write'}{$fd};
	next unless defined($ev);
	# If trigger delay is defined ...
	if(defined($ev->{'delay'}) && $ev->{'delay'} > 0) {
	    # ... schedule event handler trigger
	    $self->{'delay'}->insert($ev, $ev->{'delay'});
	# Otherwise, trigger handler immediately
	} else {
	    # Append event handler coderef to the list
	    # of event handlers that should be called
	    push @event_handlers, $ev->{'_handler'};
	    # Is there a limit on number of event triggers ?
	    next unless defined($ev->{'limit'});
	    # Reduce trigger counter
	    $ev->{'limit'}--;
	    # If trigger counter reached zero,
	    next unless ($ev->{'limit'} < 1);
	    # Append trigger limit callback coderef to the list
	    # of event handlers that should be called
	    if(defined($ev->{'_on_limit'})) {
		push @event_handlers, $ev->{'_on_limit'};
	    }
	    # Remove the event
	    $self->remove_io_event($file, 'w');
	}
    }

    # Continuous events are split into sub-second
    # time slices and processed by invoking their
    # handlers many times per second
    for(my $ev = $self->{'cont'}->first;
	defined($ev); 
	$ev = $self->{'cont'}->next($ev)) {
	# Append event handler coderef to the list
	# of event handlers that should be called
	push @event_handlers, $ev->{'_handler'};
	# Is there a limit on the number of iterations ?
	next unless defined($ev->{'limit'});
	# Reduce iteration counter
	$ev->{'limit'}--;
	# If iteration counter reached zero,
	if($ev->{'limit'} < 1) {
	    # Remove the event
	    $self->remove_continuous_event($ev);
	}
    }

    # Prepare/delay/expire timer events
    while((my $ev = $self->{'timer'}->expired())) {
	# Append event handler coderef to the list
	# of event handlers that should be called
	push @event_handlers, $ev->{'_handler'};
	# Is there a limit on the number of timer ticks ?
	if(defined($ev->{'limit'})) {
	    # Reduce tick counter
	    $ev->{'limit'}--;
	    # Do not reschedule event if
	    # counter has reached zero
	    next unless ($ev->{'limit'} > 0);
	}
	# Did event expire ?
	unless(defined($ev->{'_expire_time'}) &&
	       $ev->{'_expire_time'} < $time) {
	    # Reschedule event
	    $self->{'timer'}->insert($ev, $ev->{'interval'});
	    next;
	}
	# Append event expiration callback coderef to
	# the list of handlers that should be called
	if(defined($ev->{'_on_expiry'})) {
	    push @event_handlers, $ev->{'_on_expiry'};
	}
    }

    # Prepare all delayed events, excluding timer events
    while((my $ev = $self->{'delay'}->expired())) {
	# Delays are single-shot. Events must explicitly
	# request them when required.
	delete $ev->{'delay'};
	# I/O event delay
	if($ev->{'type'} eq 'io') {
	    # Append event handler coderef
	    # to the list of event handlers
	    # that should be called
	    push @event_handlers, $ev->{'_handler'};
	    # Is there a limit on number of event triggers ?
	    next unless defined($ev->{'limit'});
	    # Reduce trigger counter
	    $ev->{'limit'}--;
	    # If trigger counter reached zero,
	    next unless ($ev->{'limit'} < 1);
	    # Append trigger limit callback coderef to the list
	    # of event handlers that should be called
	    if(defined($ev->{'_on_limit'})) {
		push @event_handlers, $ev->{'_on_limit'};
	    }
	    # Remove the event
	    $self->remove_io_event($ev->{'file'}, $ev->{'op'});
	# Continuous event delay
	} elsif($ev->{'type'} eq 'continuous') {
	    # Append event handler coderef
	    # to the list of event handlers
	    # that should be called
	    push @event_handlers, $ev->{'_handler'};
	    # Move event (back) to
	    # its natural habitat
	    $self->{'cont'}->add($ev);
	}
    }

    return (@event_handlers);
}
#
# Invoke event handler (with timeout).
#
#  This function calls a callback function, wrapping it inside a block
#  which times its execution and interrupts it when the timer expires.
#  Handler is considered failed if it didn't complete in time.
#
#  Optionally, you can define the number of attempts to sucessfully
#  complete the execution of the handler and run a callback function
#  if all attempts have failed.
#
#  Timeout callback receives the same set of arguments as the handler.
#  This callback can complete the aborted operation and produce proper
#  return values, or simply report an error.
#
#   Input:	1. self object reference (passed implicitly)
#		2. parameter hash:
#
#		  'handler' => coderef to the handler callback (mandatory)
#		  'args' => arrayref to the callback arguments (optional)
#		  'attempts' => number of handler's attempts to complete (optional)
#		  'timeout' => single handler attempt timeout (optional)
#		  'on_timeout' => callback invoked when no attempts remain (optional)
#
#   Output:	whatever the called function returns,
#		or explicitly undef, if timed out
#
sub invoke_handler($%) {
    my $self = shift;
    my $param = {@_};

    my $scalar;
    my @array;

    my $wantarray = wantarray;

    # Callback must be coderef
    return $wantarray ? ():undef unless(defined($param->{'handler'}) &&
					ref($param->{'handler'}) eq 'CODE');

    # Unless specified, timeout is 0 (no timeout)
    my $timeout = defined($param->{'timeout'}) ?
				$param->{'timeout'}:0;

    # Unless specified, number of attempts is 1
    my $attempts = (defined($param->{'attempts'}) && $param->{'attempts'} > 1) ?
				int($param->{'attempts'}):1;

    # Unless specified otherwise, timeout handler simply aborts
    # operation in progress, leaving return values undefined
    my $to_handler = (defined($param->{'on_timeout'}) &&
		      ref($param->{'on_timeout'}) eq 'CODE') ?
			# Custom timeout handler has the chance to complete
			# operation, or to simply cleanup and/or report error
			sub {
			    unless(--$attempts) {
				if($wantarray) {
				    @array = ($param->{'on_timeout'}->(@{$param->{'args'}}));
				} else {
				    $scalar = $param->{'on_timeout'}->(@{$param->{'args'}});
				}
			    }
			    die;
			}:
			# Default timeout handler
			sub { --$attempts; die; };

    # Define temporary SIGALRM signal handler
    my $old_sigalrm = POSIX::SigAction->new();
    my $sigalrm = POSIX::SigAction->new($to_handler, POSIX::SigSet->new(SIGALRM));
    sigaction(SIGALRM, $sigalrm, $old_sigalrm);

    while($attempts) {
	# Execute handler inside sandbox
	eval {
	    eval {
		# Start timing the execution
		alarm($timeout);
		# Call event handler
		if($wantarray) {
		    @array = ($param->{'handler'}->(@{$param->{'args'}}));
		} else {
		    $scalar = $param->{'handler'}->(@{$param->{'args'}});
		}
		# Don't retry if call was completed
		$attempts = 0;
	    };
	};
    }
    # Disable alarm clock
    alarm(0);
    # Restore original signal handler
    sigaction(SIGALRM, $old_sigalrm);

    return $wantarray ? (@array):$scalar;
}

1;
