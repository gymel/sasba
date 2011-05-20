# -*- perl -*-

# t/001_load.t - check module loading and create testing directory

use Test::More tests => 9;

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


# init database
ok($use2->init(), "init database");


