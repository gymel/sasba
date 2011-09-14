# -*- perl -*-

# t/020_seealso.t - test aggregator methods only

use Test::More tests => 11;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator' );
}

# open database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');
isa_ok ($use, 'SeeAlso::Source');

# inherited methods

# description
subtest 'description' => sub {
	plan tests => 3;
        my $expected = {};
	my $descr = $use->description();
	isa_ok($descr, 'HASH', 'description');
	is(scalar keys %$descr, 0, 'description originally empty');
	is_deeply($descr, $expected, "expected description");
};

# about
subtest 'about' => sub {
	plan tests => 3;
        my $expected = ["", "", ""];
	my @about = $use->about();
	ok(@about, 'nonempty about');
	is(scalar @about, 3, 'about has three elements');
	is_deeply(\@about, $expected, "expected about");
};

# our methods

# prepare_query


# query
subtest 'undefined query' => sub {
	plan tests => 3;
	my $response = $use->query();
	isa_ok($response, "SeeAlso::Response", "Response");
	is($response->size, 0, "Empty Response");
	is($response->query, "", "Empty normalized query");
  };

subtest 'Empty query' => sub {
	plan tests => 3;
	my $response = $use->query("");
	isa_ok($response, "SeeAlso::Response", "Response");
	is($response->size, 0, "Empty Response");
	is($response->query, "", "Empty normalized query");

  };

subtest 'Arbitrary query' => sub {
	plan tests => 3;
	my $response = $use->query('XXX');
	isa_ok($response, "SeeAlso::Response", "Response");
	is($response->size, 0, "Empty Response");
	is($response->query, "XXX", "Idempotent normalized query");
};

subtest 'query existing' => sub {
	plan tests => 7;
	my $response = $use->query('118559796');
	isa_ok($response, "SeeAlso::Response", "Response");
	is($response->size, 1, "Size of response");
	is($response->query, "118559796", "normalized query");
	my($label, $description, $url) = $response->get(0);
	is($label, "Who Cares", "normalized label");
	is($description, "Who Cares", "description");
	is($url, "http://www.deutsche-biographie.de/pnd118559796.html", "url");
	is($response->toJSON(), 
	   '["118559796",["Who Cares"],["Who Cares"],["http://www.deutsche-biographie.de/pnd118559796.html"]]',
	   "JSON string");
  };

subtest 'query with filter' => sub {
	plan tests => 10;
	my ($response, $label, $description, $url);

        ok($use->set_aliasfilter("bar", "baz"), "set Filter");
	$response = $use->query('118559796');
	is($response->size, 0, "Size of filtered response");
	is($response->toJSON(), '["118559796",[],[],[]]', "JSON string");

	$response = $use->query('118784226');
	is($response->size, 1, "Size of filtered response 2");
	is($response->toJSON(), 
	   '["118784226",["???"],[""],["http://d-nb.info/gnd/118784226"]]',
	   "JSON string");

        ok($use->set_aliasfilter(), "clear Filter");

	$response = $use->query('118559796');
	is($response->size, 1, "Size of unfiltered response");
	is($response->toJSON(), 
	   '["118559796",["Who Cares"],["Who Cares"],["http://www.deutsche-biographie.de/pnd118559796.html"]]',
	   "JSON string");

	$response = $use->query('118784226');
	is($response->size, 2, "Size of unfiltered response 2");
	is($response->toJSON(), 
	   '["118784226",["Who Cares [de.wikisource.org]","???"],["Who Cares",""],["http://toolserver.org/~apper/pd/person/pnd-redirect/ws/118784226","http://d-nb.info/gnd/118784226"]]',
	   "JSON string");

  };


# findExample


