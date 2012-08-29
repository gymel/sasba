# -*- perl -*-

# t/001_load.t - check module loading and create testing directory

use Test::More tests => 16;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator' );
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
  use_ok( 'SeeAlso::Source::BeaconAggregator::Publisher' );
  use_ok( 'SeeAlso::Identifier' );
  use_ok( 'SeeAlso::Identifier::ISBN' );
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
	is($admhash->{'DATA_VERSION'}, 2, 'correct DATA_VERSION');
};

# fix identifier version
subtest 'admin hash: set IDENTIFIER_CLASS' => sub {
	plan tests => 24;
	ok($admhash = $use1->admhash(), 'get admin hash');
	is($admhash->{'IDENTIFIER_CLASS'} || "", "", 'correct IDENTIFIER_CLASS');
        my $itype1 = $use1->autoIdentifier; is((ref($itype1) || $itype1), undef);
        my $itype2 = $use2->autoIdentifier; is((ref($itype2) || $itype2), undef);

	my $object1 = new_ok("SeeAlso::Identifier::GND");
	eval { $use2->init(identifierClass => $object1) };
	is($@, "", "re-init database with identifier type");
	$admhash = $use1->admhash();
	is($admhash->{'IDENTIFIER_CLASS'} || "", ref($object1), 'correct IDENTIFIER_CLASS');
        $itype1 = $use1->autoIdentifier; isa_ok($itype1, ref($object1));
        $itype2 = $use2->autoIdentifier; isa_ok($itype2, ref($object1));

        eval { $use2->init() };
	is($@, "", "re-init database with no identifier type succeeds");
	$admhash = $use1->admhash();
	is($admhash->{'IDENTIFIER_CLASS'} || "", ref($object1), 'correct IDENTIFIER_CLASS');
        $itype1 = $use1->autoIdentifier; isa_ok($itype1, ref($object1));
        $itype2 = $use2->autoIdentifier; isa_ok($itype2, ref($object1));

        eval { $use2->init(identifierClass => $object1) };
	is($@, "", "re-init database with same identifier type succeeds");
	$admhash = $use1->admhash();
	is($admhash->{'IDENTIFIER_CLASS'} || "", ref($object1), 'correct IDENTIFIER_CLASS');
        $itype1 = $use1->autoIdentifier; isa_ok($itype1, ref($object1));
        $itype2 = $use2->autoIdentifier; isa_ok($itype2, ref($object1));

	my $object2 = new_ok("SeeAlso::Identifier");
        eval { $use2->init(identifierClass => $object2) };
	ok($@, "re-init database with a different identifier type fails");
	$admhash = $use1->admhash();
	is($admhash->{'IDENTIFIER_CLASS'} || "", ref($object1), 'correct IDENTIFIER_CLASS');
        $itype1 = $use1->autoIdentifier; isa_ok($itype1, ref($object1));
        $itype2 = $use2->autoIdentifier; isa_ok($itype2, ref($object1));

        my $use3 = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
        isa_ok ($use3, 'SeeAlso::Source::BeaconAggregator', "created db with dsn");
        my $itype3 = $use3->autoIdentifier; isa_ok($itype3, ref($object1));
};

subtest 'admin hash: remove IDENTIFIER_CLASS' => sub {
	plan tests => 19;

	my $object1 = new_ok("SeeAlso::Identifier::GND");
        eval { $use2->init(identifierClass => undef) };
	is($@, "", "re-init database with remove of identifier Class #1");
	ok($admhash = $use1->admhash(), 'reget admin hash');
	is($admhash->{'IDENTIFIER_CLASS'} || "", "", 'correctly removed IDENTIFIER_CLASS');
#       my $itype1 = $use1->autoIdentifier; isa_ok($itype1, ref($object1));
        my $itype2 = $use2->autoIdentifier; is((ref($itype2) || $itype2), undef);
# however: reopen will learn the new state
        my $use3 = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
        isa_ok ($use3, 'SeeAlso::Source::BeaconAggregator', "created db with dsn");
        my $itype3 = $use3->autoIdentifier; is((ref($itype2) || $itype2), undef);

        eval { $use2->init() };
	is($@, "", "re-init database without identifier Class");
	ok($admhash = $use1->admhash(), 'reget admin hash');
	is($admhash->{'IDENTIFIER_CLASS'} || "", "", 'correctly removed IDENTIFIER_CLASS');
        $itype2 = $use2->autoIdentifier; is((ref($itype2) || $itype2), undef);

	my $object2 = new_ok("SeeAlso::Identifier::ISBN");
        eval { $use2->init(identifierClass => $object2) };
	is($@, "", "re-init database with a different identifier type now possible");
	$admhash = $use1->admhash();
	is($admhash->{'IDENTIFIER_CLASS'} || "", ref($object2), 'correct IDENTIFIER_CLASS');
#       $itype1 = $use1->autoIdentifier; isa_ok($itype1, ref($object1));
        $itype2 = $use2->autoIdentifier; isa_ok($itype2, ref($object2));

        eval { $use2->init(identifierClass => undef) };
	is($@, "", "re-init database with remove of identifier Class #2");
	ok($admhash = $use1->admhash(), 'reget admin hash');
	is($admhash->{'IDENTIFIER_CLASS'} || "", "", 'still correctly removed IDENTIFIER_CLASS');
        $itype2 = $use2->autoIdentifier; is((ref($itype2) || $itype2), undef);

};

