# -*- perl -*-

# t/001_load.t - check module loading and create testing directory

use Test::More tests => 15;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator' );
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
  use_ok( 'SeeAlso::Source::BeaconAggregator::Publisher' );
  use_ok( 'SeeAlso::Identifier' );
  use_ok( 'SeeAlso::Identifier::GND' );
}


# mapping functions
# clone database handle
subtest 'beacon field mapping' => sub {
	plan tests => 3;
	my @fieldlist = sort SeeAlso::Source::BeaconAggregator->beaconfields();
	is(scalar @fieldlist, 21, 'twenty-one beacon fields known');
	is(SeeAlso::Source::BeaconAggregator->beaconfields('VERSION'), 'bcVERSION', 'translated correctly');
	is(SeeAlso::Source::BeaconAggregator->beaconfields('FOOBAR'), undef, 'unknown not translated');
};

subtest 'OSD field mapping' => sub {
	plan tests => 3;
	my @keylist = sort SeeAlso::Source::BeaconAggregator->osdKeys();
	is(scalar @keylist, 17, 'seventeen OSD keys known');
	is(SeeAlso::Source::BeaconAggregator->osdKeys('DateModified'), "*", 'translated correctly');
	is(SeeAlso::Source::BeaconAggregator->osdKeys('FooBar'), undef, 'unknown not translated');
};

# create new database

my $dsn = "testdb";
ok(mkdir ($dsn), "create directory for test Db");

my $dbfile = $dsn."/".$dsn."-db";

my $use1 = SeeAlso::Source::BeaconAggregator->new(file => $dbfile);
isa_ok ($use1, 'SeeAlso::Source::BeaconAggregator', "created db with file");

my $use2 = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
isa_ok ($use2, 'SeeAlso::Source::BeaconAggregator', "created db with dsn");

# init database
ok($use2->init(), "init database");

# clone database handle
subtest 'clone handle' => sub {
	plan tests => 3;
	my $hdl = DBI->connect("dbi:SQLite:dbname=$dbfile", "", "", {});
	isa_ok($hdl, DBI::db, 'direct access to SQLite database');
	diag("SQLite engine version ". $hdl->{sqlite_version});
	my $use3 = SeeAlso::Source::BeaconAggregator::Maintenance->new(hdl => $hdl);
	ok (defined $use2, "created db from handle");
	isa_ok ($use2, 'SeeAlso::Source::BeaconAggregator');
};

# get admin hash
my $admhash;
subtest 'admin hash: DATA_VERSION' => sub {
	plan tests => 3;
	ok($admhash = $use1->admhash(), 'get admin hash');
	is(ref($admhash), 'HASH', 'correct type of admin hash');
	is($admhash->{'DATA_VERSION'}, 1, 'correct DATA_VERSION');
};

# fix identifier version
subtest 'admin hash: set IDENTIFIER_CLASS' => sub {
	plan tests => 12;
	ok($admhash = $use1->admhash(), 'get admin hash');
	is($admhash->{'IDENTIFIER_CLASS'} || "", "", 'correct IDENTIFIER_CLASS');

	my $object1 = new_ok("SeeAlso::Identifier::GND");
	eval { $use2->init(identifierClass => $object1) };
	is($@, "", "re-init database with identifier type");
	$admhash = $use1->admhash();
	is($admhash->{'IDENTIFIER_CLASS'} || "", ref($object1), 'correct IDENTIFIER_CLASS');

        eval { $use2->init() };
	is($@, "", "re-init database with no identifier type succeeds");
	$admhash = $use1->admhash();
	is($admhash->{'IDENTIFIER_CLASS'} || "", ref($object1), 'correct IDENTIFIER_CLASS');

        eval { $use2->init(identifierClass => $object1) };
	is($@, "", "re-init database with same identifier type succeeds");
	$admhash = $use1->admhash();
	is($admhash->{'IDENTIFIER_CLASS'} || "", ref($object1), 'correct IDENTIFIER_CLASS');

	my $object2 = new_ok("SeeAlso::Identifier");
        my $result;
        eval { $use2->init(identifierClass => $object2) };
	ok($@, "re-init database with a different identifier type fails");
	$admhash = $use1->admhash();
	is($admhash->{'IDENTIFIER_CLASS'} || "", ref($object1), 'correct IDENTIFIER_CLASS');

};

subtest 'admin hash: remove IDENTIFIER_CLASS' => sub {
	plan tests => 9;

        eval { $use2->init(identifierClass => undef) };
	is($@, "", "re-init database with remove of identifier Class #1");
	ok($admhash = $use1->admhash(), 'reget admin hash');
	is($admhash->{'IDENTIFIER_CLASS'} || "", "", 'correctly removed IDENTIFIER_CLASS');

        eval { $use2->init() };
	is($@, "", "re-init database without identifier Class");
	ok($admhash = $use1->admhash(), 'reget admin hash');
	is($admhash->{'IDENTIFIER_CLASS'} || "", "", 'correctly removed IDENTIFIER_CLASS');

        eval { $use2->init(identifierClass => undef) };
	is($@, "", "re-init database with remove of identifier Class #2");
	ok($admhash = $use1->admhash(), 'reget admin hash');
	is($admhash->{'IDENTIFIER_CLASS'} || "", "", 'still correctly removed IDENTIFIER_CLASS');

};

