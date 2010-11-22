# -*- perl -*-

# t/001_load.t - check module loading and create testing directory

use Test::More tests => 14;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator' );
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
  use_ok( 'SeeAlso::Source::BeaconAggregator::Publisher' );
}

# create new database

my $dsn = "testdb";
ok(mkdir ($dsn), "create directory for test Db");

my $dbfile = $dsn."/".$dsn."-db";

my $use1 = SeeAlso::Source::BeaconAggregator->new(file => $dbfile);
ok (defined $use1);
isa_ok ($use1, 'SeeAlso::Source::BeaconAggregator');

my $use2 = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
ok (defined $use2, "created db with dsn");
isa_ok ($use2, 'SeeAlso::Source::BeaconAggregator');


# init and load first file
ok($use2->init(), "init database");


# load first beacon file
my ($seqno, $rec_ok, $message) = $use2->loadFile("t/beacon1.txt");
ok(defined $seqno, "load beacon file");
ok($seqno && ($seqno > 0), "something was loaded");
ok($rec_ok  && ($rec_ok > 0), "records were parsed");


undef $use1;
undef $use2;
ok(unlink $dbfile, "cleanup database");
ok(rmdir $dsn, "cleanup db dir");
