# -*- perl -*-

# t/090_maintenance.t - check beacon export

use Test::More tests => 8;

BEGIN { 
  use_ok( 'SeeAlso::Source::BeaconAggregator::Maintenance' );
}

# create new database

my $dsn = "testdb";

my $use = SeeAlso::Source::BeaconAggregator::Maintenance->new(dsn => $dsn);
ok (defined $use, "accessed db with dsn");
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator');
isa_ok ($use, 'SeeAlso::Source::BeaconAggregator::Maintenance');


# dumpmeta
subtest "deflate" => sub {
	plan tests => 1;
	ok($use->deflate(), 'deflate = VACUUM+REINDEX+ANALYZE');
};

my $file_uri;
ok($file_uri = ($use->headerfield('baz', '_uri'))[1], 'get file uri');

subtest "purge" => sub {
	plan tests => 15;
        test_counts ("pre-purge",
        	1 => ["foo", undef, "...", 3, 3],
        	5 => ["baz", $file_uri, "...", 5, 3],
          );
        ok($use->purge('baz'), 'purge');
        test_counts ("post-purge",
        	1 => ["foo", undef, "...", 3, 3],
        	5 => ["baz", $file_uri, "...", 0, 0],
          );
        ok($use->headerfield('baz', '_mtime', 0), 'reset mtime');
        test_counts ("post-purge",
        	1 => ["foo", undef, "...", 3, 3],
        	5 => ["baz", $file_uri, "", 0, 0],
          );
        # prepare forced reload
        ok($use->update('baz'), 'reload');
        test_counts ("post-update",
        	1 => ["foo", undef, "...", 3, 3],
        	8 => ["baz", $file_uri, "...", 5, 3],
          );
};

subtest "unload" => sub {
	plan tests => 5;
        test_counts ("pre-unload",
        	1 => ["foo", undef, "...", 3, 3],
        	8 => ["baz", $file_uri, "...", 5, 3],
          );
        $use->unload('baz');
        test_counts ("post-unload",
        	1 => ["foo", undef, "...", 3, 3],
          );
};


sub test_counts {
    my ($msg, %expected) = @_;
    while ( my @row = $use->listCollections() ) {
         my $seq = shift @row;
         my $exp = $expected{$seq} or fail("unexpected $seq in listCollections() [$msg]");
         $row[2] =~ s/\d+/.../;
         is_deeply(\@row, $exp, "expected stats [$msg]");
         delete $expected{$seq};
      };
    is(scalar keys %expected, 0, "all eaten up [$msg]");
}


