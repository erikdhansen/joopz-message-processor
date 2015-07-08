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
use DBI;

my $dsn = "DBI:Pg:dbname=joopz;host=127.0.0.1;port=5432";

my $LNP_URL = "http://lrn.joopz.com/get.carrier.php?tn=";
my $NO_LNP_URL = "http://199.232.41.206/getcarrier.php?p=";

print "Beginning Joopz Message Queue Processor\n";

my $client = Redis::Client->new( host => 'localhost', port => 6379 ) or die("Cannot connect to redis database!");

my $json = JSON::XS->new;

my $postgres = DBI->connect($dsn, "joopz", "joopz!") or die $DBI::errstr;

my $queue_count = $client->llen( 'messages:outgoing' );


sub get_carrier_gateway( $ ) {
    my $carrier = shift;
    print "Looking up SMTP gateway for carrier: $carrier\n";

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

    my $gateway = "";
    
    if ( $id > 0 ) {
        my $query = qq(SELECT * FROM gateways WHERE id=$id;);
        my $sth = $postgres->prepare( $query );
        if ( $sth->execute() < 0 ) {
            print "Postgres DB Error: $DBI::errstr\n";
        }
        my $row = $sth->fetchrow_hashref();
        $gateway = $row->{ hostname };
    }
    
    print "get_carrier_gateway: returning SMTP gateway $gateway\n";
}

sub get_carrier_gateway_from_nonported_phone_number( $ ) {
    my $phone = shift;
    my $query = $NO_LNP_URL . $phone;
    my $request = HTTP::Request->new(GET => $query);
    
    print "Sending request to $query...\n";
    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);
    print "Response: STATUS " . $response->status_line . "\n";
    my $carrier = $response->decoded_content;
    print "Content: " . $carrier . "\n";
    
    if ( $content == "NO" ) {
        return '';
    }
    
    return $carrier;
}

sub get_carrier_from_phone_number( $ ) {
    my $pn = shift;
    print "get_carrier_gateway_from_phone_number: raw phone = $pn\n";
    $pn =~ s/^1//g;
    print "get_carrier_gateway_from_phone_number: processed phone = $pn\n";
    
    
    my $query = $LNP_URL . $pn;
    my $request = HTTP::Request->new(GET => $query);
    
    print "Sending request to: $query...\n";
    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);
    print "Response: STATUS " . $response->status_line . "\n";
    my $carrier = lc $response->decoded_content;
    print "Content: " . $carrier . "\n";
 
    if ( $content == "Not ported" ) {
        return get_carrier_gateway_from_nonported_phone_number($pn);
    }
    return $carrier;
}

sub send_message( $ ) {
    my $msg = shift;
    my $contact_id = $msg->{ 'contact_id' };
    my $user = $msg->{ 'user_id' };
    my $message = $msg->{ 'message' };
    print "Sending message: FROM: " . $user . " TO: " . $contact_id . " MSG: " . $message . "\n";
    
    my $query = qq(SELECT * FROM contacts WHERE id=$contact_id;);
    my $sth = $postgres->prepare( $query );
    my $rv = $sth->execute() or die $DBI::errstr;
    
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
    my $to_carrier = get_carrier_gateway_from_phone_number($destPhone);
    print "   [" . $destPhone . "] => carrier = " . $to_carrier . "\n";
    my $smtphost = get_carrier_gateway($to_carrier);
    print "   [" . $toCarrier . "] => SMTP host = " . $smtphost . "\n";
    my $mailto = ( $destPhone =~ s/^1//g ) . '@' . $smtphost;
    print "MAIL TO: " . $mailto . "\n";
   
    # Build source email address
    # Get user/contact unique ID
    $query = qq(SELECT * FROM contacts WHERE id=$contact_id;); 
    $sth = $postgres->prepare( $query );
    $rv = $sth->execute();
    my $contact = $sth->fetchrow_hashref();
    my $unique_id = $contact->{ uniq_id };
    $query = qq(SELECT * FROM partners WHERE id=(SELECT parter_id FROM users WHERE phone_number='$destPhone'););
    $sth = $postgres->prepare( $query );
    $rv = $sth->execute();
    my $partner = $sth->fetchrow_hashref();
    my $partnerDomain = $partner->{ domain };
    print "[" . $destPhone . "] maps to partner " . $partnerDomain . "\n";
    
    my $mailfrom = ( $sourcePhone =~ s/^1//g ) . "." . $unique_id . "@" . $partnerDomain;
    print "MAIL FROM: " . $mailfrom . "\n";
    
    my $email = Email::MIME->create(
        header_str => [
            From   => $mailfrom,
            To     => $mailto,
        ],
        attributes => {
            encoding => "quoted_printable",
            charset  => "ISO-8859-1",
        },
        body_str => $message . "\n" . "[Sent via joopz.com]\n", 
    );
    Email::Sender::Simple qw(sendmail);
    print "WOULD DO: sendmail($email):\n" . Dumper( $email ) . "\n";
       
    #$email->{ from } = $sourcePhone;
    #$email->{ to } = $destPhone;
    #$email->{ message } = $message;
    
}

if ( $queue_count == 0 ) {
    print ("No msgs in queue.  Nothing to do.\n");
    exit;
}

print "Found $queue_count messages in outgoing queue\n";

#for ( my $i=0; $i < $queue_count; $i++ ) {
#    my $record = $client->rpop( 'messages:outgoing' );
#    my $entry = $json->decode( $record );
#    print "Sending msg[$i]...\n";
#    send_message( $entry );
#}
my $fake = {
    user_id => 138836,
    contact_id => 1306411,
    message => "Fake hard-coded message for testing.",
};

send_message( $fake );

print "Done.\n";