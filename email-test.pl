#!/usr/bin/perl
use strict;
use warnings;
use Email::MIME;

my $message = Email::MIME->create(
	header_str => [
		From	=> 'ehansen@parallelwireless.com',
		To	=> 'erik@edhkle.com',
		Subject	=> 'Testing Email::MIME',
		],
		attributes => {
			encoding => 'quoted-printable',
			charset	 => 'ISO-8859-1',
			},
		body_str => "This is a test of the Email::MIME emailing system\n",
	);

use Email::Sender::Simple qw(sendmail);
sendmail($message);

print "All done.\n";


