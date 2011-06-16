# -*- perl -*-

# t/020_stats.t - check dumps and maintenance 

use Test::More tests => 8;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
}

# open database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');

# idStat
subtest 'idStat' => sub {
	plan tests => 6;
	my $itot = $use->idStat();
	ok($itot, 'nonzero idStat');
	is($itot, 7, 'idStat returned unexpected count');
# idStat distinct
	$itot = $use->idStat(0, (distinct => 1));
	ok($itot, 'nonzero distinct idStat');
	is($itot, 5, ' distinct idStat returned unexpected count');
# idStat for one alias
	$itot = $use->idStat('foo');
	ok($itot, 'nonzero idStat for foo');
	is($itot, 3, 'idStat for foo returned unexpected count');
};


# idCounts
subtest 'idCounts' => sub {
	plan tests => 16;
	my %cexpected = (
	  '118784226' => [2, 0],
	  '132464462' => [1, 1],
	  '118624458' => [1, 2],
	  '103117741' => [2, 0],
	  '118559796' => [1, 0],
	);
	while ( my (@clist) = $use->idCounts() ) {
            my $id;
            ok($id = shift @clist, 'got list with id');
	    my $testref;
	    ok($testref = $cexpected{$id}, "idCounts returned unexpected identifier $id");
	    is_deeply(\@clist, $testref, "expected result for $id");
	    delete $cexpected{$id};
	  }
	is(scalar keys %cexpected, 0, "undelivered identifiers");
};

subtest 'idCounts distinct' => sub {
	plan tests => 16;
	my %cexpected = (
	  '118784226' => [2, 0],
	  '132464462' => [1, 1],
	  '118624458' => [1, 2],
	  '103117741' => [1, 0],
	  '118559796' => [1, 0],
	);
	while ( my (@clist) = $use->idCounts(0, (distinct => 1)) ) {
            my $id;
            ok($id = shift @clist, 'got list with id');
	    my $testref;
	    ok($testref = $cexpected{$id}, "idCounts returned unexpected identifier $id");
	    is_deeply(\@clist, $testref, "expected result for $id");
	    delete $cexpected{$id};
	  }
	is(scalar keys %cexpected, 0, "undelivered identifiers");
};

# idList
subtest 'idList' => sub {
	plan tests => 32;
	my %iexpected = (
	  '118784226' => {"1:" => ["", "", "", ""], 
        	          "3:" => ["", "de.wikisource.org", "http://toolserver.org/~apper/pd/person/pnd-redirect/ws/118784226", ""]
                	 },
	  '132464462' => {"1:" => [1, "", "", ""]},
	  '118624458' => {"1:" => [2, "", "", ""]},
	  '103117741' => {"3:45433" => ["", "Châtelain, Jean-Jacques", "", "45433"],
        	          "3:45432" => ["", "Châtelain, Jacques-Jean", "", "45432"]
	                 },
	  '118559796' => {"3:" =>, ["", "", "", ""]},
	);
	while ( my (@ilist) = $use->idList() ) {
	    ok(@ilist > 1, 'idList gave a tuple');
	    my $id = shift @ilist;
	    my $testref;
	    ok($testref = $iexpected{$id}, "idList returned unexpected identifier $id");
	    $testref ||= {};
	    while ( my $rowref = shift @ilist ) {;
	        my $seqno;
	        ok($seqno = shift @$rowref, 'defined seqno');
	        my $altid = $rowref->[3] || "";
	        my $seqref;
	        ok($seqref = $testref->{"$seqno:$altid"}, "idList returned unexpected seqno:altid $seqno:$altid");
                is_deeply($rowref, $seqref, "expected result for $id/$seqno:$altid");
	        delete $testref->{"$seqno:$altid"};
	      }
	    delete $iexpected{$id} unless keys %{$iexpected{$id}};
	  }
	is(scalar keys %iexpected, 0, "all eaten up");
};

# idList with pattern
subtest 'idList with pattern' => sub {
	plan tests => 11;
	my %iexpected = (
	  '132464462' => {"1:" => [1, "", "", ""]},
	  '118624458' => {"1:" => [2, "", "", ""]},
	);
	while ( my (@ilist) = $use->idList("%44%") ) {
	    ok(@ilist > 1, 'idList with pattern gave a tuple');
	    my $id = shift @ilist;
	    my $testref;
	    ok($testref = $iexpected{$id}, "idList with pattern returned unexpected identifier $id");
	    while ( my $rowref = shift @ilist ) {;
        	my $seqno;
	        ok($seqno = shift @$rowref, 'defined seqno');
        	my $altid = $rowref->[3];
	        my $seqref;
	        ok($seqref = $testref->{"$seqno:$altid"}, "idList with pattern returned unexpected seqno:altid $seqno:$altid");
	        $seqref ||= [];
                is_deeply($rowref, $seqref, "expected result for $id/$seqno:$altid");
	        delete $testref->{"$seqno:$altid"};
	      }
	    delete $iexpected{$id} unless keys %{$iexpected{$id}};
	  }
	is(scalar keys %iexpected, 0, "all eaten up");
};

