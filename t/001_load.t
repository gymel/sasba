# -*- perl -*-

# t/001_load.t - check module loading and create testing directory

use Test::More tests => 18;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator' );
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
  use_ok( 'SeeAlso::Source::BeaconAggregator::Publisher' );
}


# mapping functions
my @fieldlist = sort SeeAlso::Source::BeaconAggregator->beaconfields();
is(scalar @fieldlist, 21, 'twenty-one beacon fields known');
is(SeeAlso::Source::BeaconAggregator->beaconfields('VERSION'), 'bcVERSION', 'translated correctly');
is(SeeAlso::Source::BeaconAggregator->beaconfields('FOOBAR'), undef, 'unknown not translated');

my @keylist = sort SeeAlso::Source::BeaconAggregator->osdKeys();
is(scalar @keylist, 17, 'seventeen OSD keys known');
is(SeeAlso::Source::BeaconAggregator->osdKeys('DateModified'), "*", 'translated correctly');
is(SeeAlso::Source::BeaconAggregator->osdKeys('FooBar'), undef, 'unknown not translated');

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

# clone database handle
my $hdl = DBI->connect("dbi:SQLite:dbname=$dbfile", "", "", {});
isa_ok($hdl, DBI::db, 'direct access to SQLite database');
diag("SQLite engine version ". $hdl->{sqlite_version});
my $use3 = SeeAlso::Source::BeaconAggregator::Maintenance->new(hdl => $hdl);
ok (defined $use2, "created db from handle");
isa_ok ($use2, 'SeeAlso::Source::BeaconAggregator');



