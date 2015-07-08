#!/usr/bin/perl

#  joopz-msg-processor.pl
#  
#
#  Created by Erik Hansen on 6/26/15.
#

use strict;
use Redis::Client;
use JSON::XS;
use Data::Dumper;
use Email::MIME;
use HTTP::Request;
use LWP::UserAgent;
use DBI;

my $dsn = "DBI:Pg:dbname=joopz;host=127.0.0.1;port=5432";

my $LNP_URL = "http://lrn.joopz.com/get.carrier.php?tn=";
my $NO_LNP_URL = "http://199.232.41.206/getcarrier.php?p=";

print "Beginning Joopz Message Queue Processor\n";

my $client = Redis::Client->new( host => 'localhost', port => 6379 ) or die("Cannot connect to redis database!");

my $json = JSON::XS->new;

my $postgres = DBI->connect($dsn, "joopz", "joopz!") or die $DBI::errstr;

my $queue_count = $client->llen( 'messages:outgoing' );


sub carrier_gw_map_static( $ ) {
	my $carrier = shift;
	print "Using static (hard-coded) carrier name to gateway ID mappings: $carrier\n";
	
	my $id = 0;
    
    if ( $carrier =~ /verizon/ ) {
        $id = 1;
    }
    
    if ( $carrier =~ /cingular/ || $carrier =~ /at\&t/ || $carrier =~ /att/ ) {
        $id = 3;
    }
    
    if ( $carrier =~ /uscc/ ) {
        $id = 4;
    }
    
    if ( $carrier =~ /alltel/ ) {
        $id = 5;
    }
    
    if ( $carrier =~ /cellularone/ || $carrier =~ /cellular one/ ) {
        $id = 9;
    }
    
    if ( $carrier =~ /t\-mobile/ || $carrier =~ /tmobile/ ) {
        $id = 7;
    }
    
    if ( $carrier =~ /metro/ ) {
        $id = 15;
    }
    
    if ( $carrier =~ /cricket/ ) {
        $id = 11;
    }
    
    if ( $carrier =~ /century/ ) {
        $id = 12;
    }
    
    if ( $carrier =~ /boost/ ) {
        $id = 19;
    }
    
    if ( $carrier =~ /sprint/ ) {
        $id = 2;
    }

	return $id
}

sub carrier_gw_map_postgres( $ ) {
	my $carrier = shift;
	print "Using PostgreSQL database (carrier table) for carrier to gateway mappings: $carrier\n";
	
	my $gatewayId = 0;
	my $query = qq(SELECT * FROM carriers WHERE LOWER(name) like '%$carrier%';);
	my $sth = $postgres->prepare( $query );
	if ( $sth->execute < 0 ) {
		print "Postgres DB Error! $DBI::errstr\n";
		return $gatewayId;
	}
	my $row = $sth->fetchrow_hashref();
	$gatewayId = $row->{ gateway_id };
	return $gatewayId;
}

sub get_carrier_gateway( $ ) {
    my $carrier = shift;
    print "Looking up SMTP gateway for carrier: $carrier\n";

	my $gwId = carrier_gw_map_static( $carrier );
	print "carrier_gw_map_static(" . $carrier . ") => gatewayId : " . $gwId . "\n";
	if( $gwId == 0 ) {
		$gwId = carrier_gw_map_postgres( $carrier );
		print "carrier_gw_map_static(" . $carrier . ") => gatewayId : " . $gwId . "\n";
	}
	
    my $gateway = "";
    
    if ( $gwId > 0 ) {
        my $query = qq(SELECT * FROM gateways WHERE id=$gwId;);
        my $sth = $postgres->prepare( $query );
        if ( $sth->execute() < 0 ) {
            print "Postgres DB Error: $DBI::errstr\n";
        }
        my $row = $sth->fetchrow_hashref();
        $gateway = $row->{ hostname };
    }
    
    print "get_carrier_gateway: returning SMTP gateway $gateway\n";
    return $gateway;
}

sub get_carrier_from_nonported_phone_number( $ ) {
    my $phone = shift;
    my $query = $NO_LNP_URL . $phone;
    my $request = HTTP::Request->new(GET => $query);
    
    print "Sending request to $query...\n";
    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);
    print "Response: STATUS " . $response->status_line . "\n";
    my $carrier = $response->decoded_content;
    print "Content: " . $carrier . "\n";
    
    if ( $carrier == "NO" ) {
        return '';
    }
    
    return lc $carrier;
}

sub get_carrier_from_phone_number( $ ) {
    my $pn = shift;
    print "get_carrier_from_phone_number: raw phone = $pn\n";
    $pn =~ s/^1//g;
    print "get_carrier_from_phone_number: processed phone = $pn\n";
    
    
    my $query = $LNP_URL . $pn;
    my $request = HTTP::Request->new(GET => $query);
    
    print "Sending request to: $query...\n";
    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);
    print "Response: STATUS " . $response->status_line . "\n";
    my $carrier = lc $response->decoded_content;
    $carrier =~ s/^\n//g;
    chomp($carrier);
    
    print "Content: " . $carrier . "\n";
 
    if ( $carrier eq "" || $carrier =~ /not ported/ ) {
        return get_carrier_from_nonported_phone_number($pn);
    }
    return $carrier;
}

sub update_conversation( $$ ) {
	my $userId = shift;
	my $contactId = shift;
	
	my $query = qq(SELECT * FROM conversations WHERE user_id=$userId AND contact_id=$contactId;);
	my $sth = $postgres->prepare( $query );
	my $rv = $sth->execute();
	
	if( $rv < 0 ) {
		print "Postgres DB Error Updating Conversation State! $DBI::errstr\n";
	} else {
		my $conversation = $sth->fetchrow_hashref();
		my $conversationId = $conversation->{ id };
		my $timestamp = time;
		my $convo = {
			conversation_id	=> $conversationId,
			UserId => $userId,
			ContactId => $contactId,
			Timestamp => $timestamp,
			Archived => "false",
		};
		
		my $json_convo = $json->encode( $convo );
		$client->lpush( 'conversations:updated', $json_convo );
		print "Pushed conversation descriptor onto queue:\n" . Dumper( $json_convo ) . "\n";
	}
}

sub send_message( $ ) {
    my $msg = shift;
    my $contact_id = $msg->{ 'contact_id' };
    my $user = $msg->{ 'user_id' };
    my $message = $msg->{ 'message' };
    print "Sending message: FROM: " . $user . " TO: " . $contact_id . " MSG: " . $message . "\n";
    
    my $query = qq(SELECT * FROM contacts WHERE id=$contact_id;);
    my $sth = $postgres->prepare( $query );
    my $rv = $sth->execute();
    
    if ( $rv < 0 ) {
        print $DBI::errstr;
    }
    
    my $row = $sth->fetchrow_hashref();
    my $destPhone = $row->{ phone_number };
 
    $query = qq(SELECT * FROM users WHERE id=$user;);
    $sth = $postgres->prepare( $query );
    $rv = $sth->execute() or die $DBI::errstr;
    
    $row = $sth->fetchrow_hashref();
    
    my $sourcePhone = $row->{ phone_number };
    
    my $email = {};
    # get required ingredients
    # Build destination email address
    print "Building MAIL TO address for phone: $destPhone\n";
    my $to_carrier = get_carrier_from_phone_number($destPhone);
    my $smtphost = get_carrier_gateway($to_carrier);
    
    if ( $smtphost eq "" ) {
		# Return from here and this message gets dropped and not sent
		print "!!! FATAL Error !!!\nNo available SMTP gateway for carrier: $to_carrier\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
		return;
	}
	
    print "[" . $destPhone . "] => [" . $to_carrier . "]: SMTP host " . $smtphost . "\n";
    my $mailto = $destPhone . '@' . $smtphost;
    print "MAILTO: $mailto\n";
    if ( $mailto =~ s/^1//g ) {
        print "Stripped leading 1: $mailto\n";
    } else {
        print "Nothing to strip: $mailto\n";
    }
   
    # Build source email address
    # Get user/contact unique ID
    $query = qq(SELECT * FROM contacts WHERE id=$contact_id;); 
    $sth = $postgres->prepare( $query );
    $rv = $sth->execute();
    my $contact = $sth->fetchrow_hashref();
    my $unique_id = $contact->{ uniq_id };
    $query = qq(SELECT domain FROM partners WHERE id=(SELECT partner_id FROM users WHERE phone_number='$sourcePhone'););
    $sth = $postgres->prepare( $query );
    $rv = $sth->execute();
    my $partner = $sth->fetchrow_hashref();
    my $partnerDomain = $partner->{ domain };
    print "[" . $sourcePhone . "] maps to partner " . $partnerDomain . "\n";

    my $mailfrom = $sourcePhone . "." . $unique_id . "@" . $partnerDomain;
    
    if ( $mailfrom =~ s/^1//g ) {
        print "Stripped leading 1: $mailfrom\n";
    } else {
        print "Nothing to strip: $mailfrom\n";
    }
    
    print "MAIL FROM: " . $mailfrom . "\n";
    
    my $email = Email::MIME->create(
        header_str => [
            From   => $mailfrom,
            To     => $mailto,
        ],
        attributes => {
            encoding => "quoted-printable",
            charset  => "ISO-8859-1",
        },
        body_str => $message . "\n" . "[Sent via joopz.com]\n", 
    );
    use Email::Sender::Simple qw(sendmail);
    sendmail($email);
    
    update_conversation( $user, $contact_id );
    
    print "Email sent FROM: $mailfrom  TO: $mailto  MSG: $message\n";
}

print "Joopz Redis Database Starting Up...\n";
print "Queue[messages:outgoing]: $queue_count messages waiting in the queue\n";

while ( my ( $queue, $json_request ) = $client->brpop( 'messages:outgoing', 0 )) {
    print ">>> Pulled new SMS request from $queue!\nRequest:\n" . Dumper( $json_request );
    my $request = $json->decode( $json_request );
    send_message( $request );
    print "<<< Done with request: contactId=" . $request->{ contact_id } . " userId=" . $request->{ user_id } . " Msg: " . $request->{ message } . "\n";
}

#for ( my $i=0; $i < $queue_count; $i++ ) {
#    my $record = $client->rpop( 'messages:outgoing' );
#    my $entry = $json->decode( $record );
#    print "Sending msg[$i]...\n";
#    send_message( $entry );
#}

#my $fake = {
#    user_id => 138836,
#    contact_id => 1306411,
#    message => "Fake hard-coded message for testing.",
#};

#send_message( $fake );

print "Done.\n";
