# -*- perl -*-

# t/010_beacon.t - check module loading and create testing directory

use Test::More tests => 6;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
}

# create new database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
ok (defined $use, "created db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');


# load first beacon file
my ($seqno, $rec_ok, $message) = $use->loadFile("t/beacon1.txt");
ok(defined $seqno, "load beacon file");
ok($seqno && ($seqno > 0), "something was loaded");
ok($rec_ok  && ($rec_ok > 0), "records were parsed");
