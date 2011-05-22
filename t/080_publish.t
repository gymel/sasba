# -*- perl -*-

# t/080_publish.t - publisher functions: 

use Test::More tests => 8;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Publisher' );
}

# create new database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Publisher->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator::Publisher');


subtest "cast existing object" => sub {
	plan tests => 3;
	my $use2 = SeeAlso::Source::BeaconAggregator->new(dsn => $dsn);
	ok (defined $use2, "accessed db with dsn");
	isa_ok ($use2, 'SeeAlso::Source::BeaconAggregator');
	SeeAlso::Source::BeaconAggregator::Publisher->activate($use2);
	isa_ok ($use2, 'SeeAlso::Source::BeaconAggregator::Publisher');
};

subtest "get_meta" => sub {
	plan tests => 5;
	my $expected_osd = {
		Tags => ["hits", "some encountered"],
	};
	my $expected_meta = {
		MESSAGE => ' encountered',
	};
	my ($osd, $meta);
	ok( ($osd, $meta) = $use->get_meta() );
        isa_ok($osd, 'HASH');
	is_deeply($osd, $expected_osd, 'OSD as expected');
        isa_ok($meta, 'HASH');
	is_deeply($meta, $expected_meta, 'Beacon meta fields as expected');
};

# redirect
subtest "redirect" => sub {
	plan tests => 1;
	my $use2 = SeeAlso::Source::BeaconAggregator::Publisher->new(dsn => $dsn);
TODO: {
	local $TODO = "test not implemented";
	ok(0);
}
};

# sources
subtest "sources" => sub {
	plan tests => 1;
	my $use2 = SeeAlso::Source::BeaconAggregator::Publisher->new(dsn => $dsn);
TODO: {
	local $TODO = "test not implemented";
	ok(0);
}
};


