#!/usr/bin/perl
use strict;
use warnings;
<<<<<<< HEAD

use Email::MIME;
use Email::Sender::Transport::SMTP;
=======
use Email::MIME;
>>>>>>> ffc72aeeabfbce754f6d6a6b437e43050d56b6e2

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


