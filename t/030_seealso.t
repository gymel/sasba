# -*- perl -*-

# t/030_seealso.t - test aggregator methods only

use Test::More tests => 14;

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

subtest 'query existing plain' => sub {
	plan tests => 7;
	my $response = $use->query('118559796');
	isa_ok($response, "SeeAlso::Response", "Response");
	is($response->size, 1, "Size of response");
	is($response->query, "118559796", "normalized query");

	my($label, $description, $url) = $response->get(0);
	is($label, "I Cared", "normalized label");
	is($description, "I Cared", "description");
	is($url, "http://www.deutsche-biographie.de/pnd118559796.html", "url");

	is($response->toJSON(), 
	   '["118559796",["I Cared"],["I Cared"],["http://www.deutsche-biographie.de/pnd118559796.html"]]',
	   "JSON string");
  };

subtest 'query existing explicit' => sub {
	plan tests => 10;
	my $response = $use->query('118784226');
	isa_ok($response, "SeeAlso::Response", "Response");
	is($response->size, 2, "Size of response");
	is($response->query, "118784226", "normalized query");

	my($label, $description, $url) = $response->get(0);
	is($label, "I Cared [de.wikisource.org]", "normalized label (0)");
	is($description, "I Cared", "description (0)");
	is($url, "http://toolserver.org/~apper/pd/person/pnd-redirect/ws/118784226", "url (0)");

	($label, $description, $url) = $response->get(1);    # from foo
	is($label, "???", "normalized label (1)");
	is($description, "foo", "description (1)");
	is($url, "http://d-nb.info/gnd/118784226", "url (1)");

	is($response->toJSON(), 
	   '["118784226",["I Cared [de.wikisource.org]","???"],["I Cared","foo"],["http://toolserver.org/~apper/pd/person/pnd-redirect/ws/118784226","http://d-nb.info/gnd/118784226"]]',
           "JSON string");
  };

subtest 'query existing altid' => sub {
	plan tests => 13;
	my $response = $use->query('103117741');
	isa_ok($response, "SeeAlso::Response", "Response");
	is($response->size, 3, "Size of response");
	is($response->query, "103117741", "normalized query");

	my($label, $description, $url) = $response->get(0);
	is($label, "I Cared [Châtelain, Jean-Jacques]", "normalized label (0)");
	is($description, "I Cared", "description (0)");
	is($url, "http://www.hls-dhs-dss.ch/textes/d/D45433.php", "url (0)");

	($label, $description, $url) = $response->get(1);
	is($label, "I Cared [Test encoding only]", "normalized label (1)");
	is($description, "I Cared", "description (1)");
	is($url, "http://www.hls-dhs-dss.ch/textes/d/DT%C3%A2t%C3%A2.php", "url (1)");

	($label, $description, $url) = $response->get(2);
	is($label, "I Cared [Châtelain, Jacques-Jean]", "normalized label (2)");
	is($description, "I Cared", "description (2)");
	is($url, "http://www.hls-dhs-dss.ch/textes/d/D45432.php", "url (2)");

	is($response->toJSON(), 
	   '["103117741",["I Cared [Châtelain, Jean-Jacques]","I Cared [Test encoding only]","I Cared [Châtelain, Jacques-Jean]"],["I Cared","I Cared","I Cared"],["http://www.hls-dhs-dss.ch/textes/d/D45433.php","http://www.hls-dhs-dss.ch/textes/d/DT%C3%A2t%C3%A2.php","http://www.hls-dhs-dss.ch/textes/d/D45432.php"]]',
	   "JSON string");
  };

subtest 'query existing new altid' => sub {
	plan tests => 7;
	my $response = $use->query('100001718');
	isa_ok($response, "SeeAlso::Response", "Response");
	is($response->size, 1, "Size of response");
	is($response->query, "100001718", "normalized query");

	my($label, $description, $url) = $response->get(0);
	is($label, "SUDOC", "normalized label (0)");
	is($description, "Mapping from GND IDs to SUDOC IDs (via Wikidata Q-Items) [Q533022]", "description (0)");
	is($url, "http://www.idref.fr/117503258", "url (0)");

	is($response->toJSON(), 
	   '["100001718",["SUDOC"],["Mapping from GND IDs to SUDOC IDs (via Wikidata Q-Items) [Q533022]"],["http://www.idref.fr/117503258"]]',
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
	   '["118784226",["???"],["foo"],["http://d-nb.info/gnd/118784226"]]',
	   "JSON string");

        ok($use->set_aliasfilter(), "clear Filter");

	$response = $use->query('118559796');
	is($response->size, 1, "Size of unfiltered response");
	is($response->toJSON(), 
	   '["118559796",["I Cared"],["I Cared"],["http://www.deutsche-biographie.de/pnd118559796.html"]]',
	   "JSON string");

	$response = $use->query('118784226');
	is($response->size, 2, "Size of unfiltered response 2");
	is($response->toJSON(), 
	   '["118784226",["I Cared [de.wikisource.org]","???"],["I Cared","foo"],["http://toolserver.org/~apper/pd/person/pnd-redirect/ws/118784226","http://d-nb.info/gnd/118784226"]]',
	   "JSON string");

  };


# findExample


