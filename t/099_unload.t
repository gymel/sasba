# -*- perl -*-

# t/099_unload.t - discard testing directory

use Test::More tests => 2;

my $dsn = "testdb";
my $dbfile = $dsn."/".$dsn."-db";

ok(unlink $dbfile, "cleanup database");
ok(rmdir $dsn, "cleanup db dir");
