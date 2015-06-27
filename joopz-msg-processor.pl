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
use DBI;

my $dsn = "DBI:Pg:dbname=joopz;host=127.0.0.1;port=5432";

print "Beginning Joopz Message Queue Processor\n";

my $client = Redis::Client->new( host => 'localhost', port => 6379 ) or die("Cannot connect to redis database!");

my $json = JSON::XS->new;

my $postgres = DBI->connect($dsn, "joopz", "joopz!") or die $DBI::errstr;

my $queue_count = $client->llen( 'messages:outgoing' );


sub send_message( $ ) {
    my $msg = shift;
    my $contact = $msg->{ 'contact_id' };
    my $user = $msg->{ 'user_id' };
    my $message = $msg->{ 'message' };
    print "Sending message: FROM: $user TO: $contact MSG: $message\n";
    
    my $query = qq(SELECT * FROM contacts WHERE id=$contact;);
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
    $email->{ from } = $sourcePhone;
    $email->{ to } = $destPhone;
    $email->{ message } = $message;
    
    print Dumper( $email );
}

if ( $queue_count == 0 ) {
    print ("No msgs in queue.  Nothing to do.\n");
    exit;
}

print "Found $queue_count messages in outgoing queue\n";

for ( my $i=0; $i < $queue_count; $i++ ) {
    my $record = $client->rpop( 'messages:outgoing' );
    my $entry = $json->decode( $record );
    print "Sending msg[$i]...\n";
    send_message( $entry );
}

print "Done.\n";