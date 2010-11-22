package SeeAlso::Source::BeaconAggregator::Maintenance;
use strict;
use warnings;

BEGIN {
    use Exporter ();
    use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);
    $VERSION     = '0.1';
    @ISA         = qw(Exporter);
    #Give a hoot don't pollute, do not export more than needed by default
    @EXPORT      = qw();
    @EXPORT_OK   = qw();
    %EXPORT_TAGS = ();
}

use base ("SeeAlso::Source::BeaconAggregator");
use Carp;
use HTTP::Date;     # not perfect, but the module is commonly installed...
use HTTP::Request;
use File::Temp;

=head1 NAME

sasbactrl.pl - command line interface to SeeAlso::Source::BeaconAggregator and
               auxiliary classes

=head1 SYNOPSIS


=head1 DESCRIPTION

This Module allows a collection of BEACON files (cf. http://de.wikipedia.org/wiki/Wikipedia:PND/BEACON)
to be used as SeeAlso::Source (probably in the context of an SeeAlso::Server application).
Therefore it implements the four methods documented in SeeAlso::Source

The BEACON files (lists of non-local identifiers of a certain type documenting the coverage of a given 
online database plus means for access) are imported by the methods provided by 
SeeAlso::Source::BeaconAggregator::Maintenance.pm, usually by employing the script sasbactrl.pl
as command line client.

Serving other formats than SeeAlso or providing a BEACON file with respect to this
SeeAlso service is achieved by using SeeAlso::Source::BeaconAggregator::Publisher.


=head1 USAGE


=head2 SeeAlso::Source::BeaconAggregator Methods

=head3 init()

Sets up and initializes the database for the object.

=cut

sub init {
  my ($self) = @_;

  my @fieldlist = SeeAlso::Source::BeaconAggregator->beaconfields();
  my @bf = map{ join(" ", @{[SeeAlso::Source::BeaconAggregator->beaconfields($_)]}[0..1]) } @fieldlist;
  my $hdl = $self->{dbh} or croak("no database handle?");

  local($") = ",\n";
  $hdl->do(<<"XxX"
CREATE TABLE IF NOT EXISTS repos (
    seqno INTEGER PRIMARY KEY AUTOINCREMENT,
    alias TEXT,
    sort TEXT,
    uri VARCHAR(512),
    ruri VARCHAR(512),
    mtime INTEGER,
    utime INTEGER,
    ftime INTEGER,
    counti INTEGER DEFAULT 0,
    countu INTEGER DEFAULT 0,
    fstat TEXT,
    ustat TEXT,
    admin VARCHAR(512),
    @bf
);
XxX
    ) or croak("Setup error: ".$hdl->errstr);

  $hdl->do("CREATE UNIQUE INDEX IF NOT EXISTS seqnos ON repos(seqno);") or croak("Setup error: ".$hdl->errstr);
  $hdl->do("CREATE INDEX IF NOT EXISTS aliases ON repos(alias);") or croak("Setup error: ".$hdl->errstr);

  my ($at, $type) = SeeAlso::Source::BeaconAggregator->beaconfields("COUNT");
  $hdl->do("ALTER TABLE repos ADD COLUMN $at $type;");

  ($at, $type) = SeeAlso::Source::BeaconAggregator->beaconfields("REMARK");
  $hdl->do("ALTER TABLE repos ADD COLUMN $at $type;");

  $hdl->do(<<"XxX"
CREATE TABLE IF NOT EXISTS beacons (
    hash CHARACTER(64) NOT NULL,
    seqno INTEGER REFERENCES repos(seqno) ON DELETE CASCADE,
    altid TEXT,
    hits INTEGER,
    info VARCHAR(255),
    link VARCHAR(1024)    
);
XxX
    ) or croak("Setup error: ".$hdl->errstr);


# Faciliate lookups
  $hdl->do("CREATE INDEX IF NOT EXISTS ref ON beacons(hash);") or croak("Setup error: ".$hdl->errstr);
# enforce constraints
  $hdl->do("CREATE UNIQUE INDEX IF NOT EXISTS hshrepalt ON beacons(hash, seqno, altid);") or croak("Setup error: ".$hdl->errstr);

# foreign key on cascade does not work?

  $hdl->do(<<"XxX"
CREATE TRIGGER IF NOT EXISTS on_delete_seqno BEFORE DELETE ON repos FOR EACH ROW
 BEGIN
  DELETE FROM beacons WHERE seqno=OLD.seqno;
 END;
XxX
    ) or croak("Setup error: ".$hdl->errstr);

# OpenSearchDescription
  $hdl->do(<<"XxX"
CREATE TABLE IF NOT EXISTS osd (
    key CHAR(20) NOT NULL,
    val VARCHAR(1024)
);
XxX
    ) or croak("Setup error: ".$hdl->errstr);

  $hdl->do("CREATE INDEX IF NOT EXISTS OSDKeys ON osd(key);") or croak("Setup error: ".$hdl->errstr);

  $hdl->do("ANALYZE;");

  return 1;    # o.k.
};

=head3 deflate()

Maintenance action: performs VACCUUM, REINDEX and ANALYZE on the database

=cut

sub deflate {
  my ($self) = @_;
  my $hdl = $self->{dbh} or croak("no handle?");
  print "VACUUM\n";
  $hdl->do("VACUUM") or croak("could not VACUUM: Abort");
  print "REINDEX\n";
  $hdl->do("REINDEX") or croak("could not REINDEX: Abort");
  print "ANALYZE\n";
  $hdl->do("ANALYZE;") or croak("could not ANALYZE: Abort");
  return 1;
}

=head3 ($seqno, $rec_ok, $message) = loadFile ( $file, $fields, %options ) 

Reads a physical beacon file and stores it with a new Sequence number in the
database.

$seqno is undef on error

$seqno and $rec_ok are zero with $message containing an explanation in case
of no action taken

$seqno is an positive integer if something was loaded.

=over 8

=item $file

File to read: Must be a beacon file

=item $fields

Hashref with additional meta and admin fields to store

=item Supported options: 

 verbose => (0|1)

=back

If the file does not contain a minimal correct header (eg. is an empty file 
or an HTML error page accidentaly caught) no action is performed.

Otherwise, a fresh SeqNo (sequence number) is generated and meta and
BEACON-Lines are stored in the appropriate tables in the database.

If the _alias field is provided, existing database entries for this
Alias are updated, identifiers not accounted for any more are 
eventually discarded. 

=cut
sub loadFile {
  my ($self, $file, $fields, %options) = @_;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};
  if ( ! $file ) {
      croak("Missing file argument")}
  elsif ( ! -r $file ) {
      print "ERROR: no such file $file\n";
      return undef;
    }
  my $mtime = (stat(_))[9];
  open(BKN, "<:utf8", $file) or (print "ERROR: cannot read $file\n", return undef);

  $fields = {} unless $fields;
  $fields->{'_ftime'} ||= time();
  $fields->{'_mtime'} ||= $mtime;
  delete $fields->{_uri} unless $fields->{_uri};
  delete $fields->{_alias} unless $fields->{_alias};
  my $autopurge = $fields->{_alias} || "";
  my $showme = $fields->{_alias} || $fields->{_uri} || $file;

  if ( $options{'verbose'} ) {
      printf("* Loading %s from URI %s\n", $fields->{_alias} || "<no alias>", $fields->{_uri} || "<direct file>");
      printf("* local input %s (%s)\n", $file, SeeAlso::Source::BeaconAggregator::tToISO($mtime));
    };

  my ($collno, $inserthandle, $replacehandle, $err, $format);
  my ($linecount, $headerseen, $oseq) = (0, 0, 0);
  my ($reccount, $recill, $recign, $recnil, $recupd, $recnew, $recdupl, $recdel) = (0, 0, 0, 0, 0, 0, 0, 0);
  local($_);
  lines:
  while ( <BKN> ) {
      s/[ \x0d\x0a]+$//;
      unless ( $linecount++ ) {
          if ( s/^\x{FEFF}// ) { # BOM-Character
            }
          elsif ( s/^\xef\xbb\xbf// ) { # BOM-Bytes
              print "ERROR: cannot cope with doubly UTF-8 encoded $file\n";
              return (undef, undef, "encoding trouble")};
          if ( /^\s*$/ ) {
              print "WARNING: Discarding blank line before beacon header [$showme l.$.]\n";
              next;
            };
        };
      if ( not defined $collno ) {      # $collno used as flag: "still in header"
          if ( /^#\s*(\w+):\s*(.*)$/ ) {
              $headerseen++;
              my ($field, $data) = ($1, $2);
              $field =~ s/^DATE$/TIMESTAMP/ && print "WARNING: corrected DATE to TIMESTAMP in Beacon-Header [$showme l.$.]\n";
              $data =~ s/\s+$//;
              next if $data =~ /^\s*$/;
              if ( SeeAlso::Source::BeaconAggregator->beaconfields($field) ) {
                  if ( $fields->{$field} ) {
                      print "WARNING: Skipping already set $field [$showme l.$.]\n"}
                  else {
                      $fields->{$field} = $data}
                }
              else {
                  print "WARNING: Ignoring unknown $field [$data] [$showme l.$.]\n";
                };
            }
          elsif ( /^\s*$/ ) {
              print "NOTICE: Discarding blank line in beacon header context [$showme l.$.]\n" if $options{'verbose'}}
          elsif ( ! $headerseen ) {
              print "ERROR: no header fields [$showme l.$.]\n";
              return (0, 0, "no header fields: Will not proceed");
            }
          else {
              ($collno, $err, $format, $inserthandle, $replacehandle, $oseq) = $self->processbeaconheader($fields, %options);
              unless ( $collno ) {
                  print "ERROR: metadata error [$showme l.$.]\n";
                  return (0, 0, "metadata error: $err");
                };
              $self->{dbh}->{AutoCommit} = 0;
              $linecount --;
              redo lines;
            }
        }
      else {
         s/^\s+//;
         my ($id, $altid, @rest);
         ($id, @rest) = split(/\s*\|\s*/, $_, 4);
         ($id, $altid) = split(/\s*=\s*/, $id, 2) if $id;
         $id || ($recnil++, next);
         $altid ||= "";

         my($hits, $info, $link);
         if ( @rest && ($rest[$#rest] =~ m!^\S+://\S+$!) ) {
             $link = pop @rest}
         elsif ( defined $rest[2] ) {
             print "WARNING: unparseable link content >$rest[2]< [$showme l.$.]"};

         if ( @rest && ($rest[0] =~ /^\d*$/) ) {
             $hits = shift @rest;
                                    # really throw out hits with explicit 0?
             $info = shift @rest || "";
           }
         elsif ( defined $rest[1] ) {
             $hits = "", shift @rest;
             $info = shift @rest;
           }
         elsif ( defined $rest[0] ) {
             $hits = "";
             $info = shift @rest;
             shift @rest;
           };
         if ( @rest ) {
             print "WARNING: unparseable content >$_< [$showme l.$.]"};

         unless ( $link ) {
             if ( $format =~ /\baltTARGET\b/ ) {
                 unless ( $altid ) {
                     print "NOTICE: discarding >$id< ($hits) without altid nor link [$showme l.$.]\n";  # if $options{'verbose'};
                     $recill++;
                     next lines;
                   }
               }
             elsif ( $format =~ /\bnoTARGET\b/ ) {
                 print "NOTICE: discarding >$id< ($hits) without link [$showme l.$.]\n" if $options{'verbose'} > 1;
                 $recill++;
                 next lines;
               }
           };

         if ( $format !~ /\baltTARGET\b/ ) {            # Allow certain duplicates (force disambiguization)
             $altid ||= $info || $link}

         $hits = "" unless defined $hits;
         ($hits =~ /^0+/) && ($recnil++, next);          # Explizit "0" => raus
         $hits = 0 if $hits eq "";
         $altid ||= "";
         my $hash;
         if ( exists $self->{identifierClass} ) {
             $self->{identifierClass}->value($id);
             unless ( $self->{identifierClass}->valid ) {
                 print "NOTICE: invalid identifier >$id< ($hits) [$showme l.$.]\n" if $options{'verbose'};
                 $recill++;
                 next lines;
               };
             $hash = $self->{identifierClass}->hash();
           }
         else {
             $hash = $id};
         my $did;
         if ( $replacehandle && ($did = $replacehandle->execute($hits, $info, $link, $hash, $altid)) ) { # UPDATE OR FAIL old record
             if ( $replacehandle->err ) {
carp("update in trouble: $replacehandle->errstring [$showme l.$.]");
                 $recdupl++;
               }
             elsif ( $did eq "0E0" ) {  # not found, try insert
                 $did = $inserthandle->execute($hash, $altid, $hits, $info, $link);
                 if ( $did eq "0E0" ) {
                     $recdupl++;
                     if ( $altid ) {
                         print "INFO: did not insert duplicate Id >$id< = >$altid< ($hits) [$showme l.$.]\n" if $options{'verbose'}}
                     else {
                         print "INFO: did not insert duplicate Id >$id< ($hits) [$showme l.$.]\n" if $options{'verbose'} > 1};
                   }
                 else {
                     $recnew++};
               }
             else {
                 $recupd++};
           }
         elsif ( $did = $inserthandle->execute($hash, $altid, $hits, $info, $link) ) { # INSERT OR IGNORE new record
             if ( $did eq "0E0" ) {
                 $recdupl++;
                 print "INFO: did not insert duplicate Id $id ($hits) [$showme l.$.]\n" if $options{'verbose'} > 1;
               }
             else {
                 $recnew++};
           }
         elsif ( $inserthandle->errstr =~ /constraint/ ) {
             $recdupl++;
             print "INFO: duplicate Id $id ($hits): not inserting [$showme l.$.]\n" if $options{'verbose'} > 1;
           }
         else {
             croak("Could not insert: ($id, $hits, $info, $link): ".$inserthandle->errstr)};
         unless ( ++$reccount % 10000 ) {
             $self->{dbh}->{AutoCommit} = 1;
             print "$reccount\n" if $options{'verbose'};
             $self->{dbh}->{AutoCommit} = 0;
           };
        }
    };
  if ( not defined $collno ) {
      if ( $headerseen ) {
          ($collno, $err, $format, $inserthandle, $replacehandle, $oseq) = $self->processbeaconheader($fields, %options);
          if ( $collno ) {
              print "WARNING: no idn content in file [$showme l.$.]\n"}
          else {
              print "ERROR: metadata error [$showme l.$.]\n";
              return (0,0, "metadata error: $err");
            };
        }
      elsif ( $. ) {
          print "ERROR: no header fields [$showme l.$.]\n";
          return (0, 0, "no header fields: Will not proceed");
        }
      else {
          print "WARNING: empty file [$showme]\n";
          return (0,0, "empty file");
        };
    }
  $self->{dbh}->{AutoCommit} = 1;

  if ( $autopurge ) {
      $self->{dbh}->{AutoCommit} = 0;
      if ( $oseq ) {
          my $bcdelsql = "DELETE FROM beacons WHERE seqno==?";
          my $bcdelh = $self->{dbh}->prepare($bcdelsql) or croak("Could not prepare $bcdelsql: ".$self->{dbh}->errstr);
          my $rows = $bcdelh->execute($oseq) or croak("Could not execute $bcdelsql: ".$bcdelh->errstr);
          $self->{dbh}->{AutoCommit} = 1;
          printf("INFO: Purged %s surplus identifiers from old sequence %u\n", $rows, $oseq) if $options{'verbose'};
          $rows = "0" if $rows eq "0E0";
          $recdel += $rows;
        };

      $self->{dbh}->{AutoCommit} = 0;
      my $rpdelsql = "DELETE FROM repos WHERE (alias=?) AND (seqno<?);";
      my $rpdelh = $self->{dbh}->prepare($rpdelsql) or croak("Could not prepare $rpdelsql: ".$self->{dbh}->errstr);
      my $rows = $rpdelh->execute($autopurge, $collno) or croak("Could not execute $rpdelsql: ".$rpdelh->errstr);
      $self->{dbh}->{AutoCommit} = 1;
      $rows = "0" if $rows eq "0E0";
      printf("INFO: %u old sequences discarded\n", $rows) if $options{'verbose'};
    }

  printf "NOTICE: New sequence %u for %s: processed %u Records from %u lines\n",
                          $collno, $autopurge || "???", $reccount,  $linecount;
  my $statline = sprintf "%u replaced, %u new, %u deleted, %u duplicate, %u nil, %u invalid, %u ignored",
                          $recupd,     $recnew, $recdel,   $recdupl,     $recnil, $recill,   $recign;
  print "       ($statline)\n";

  my $recok = $recupd + $recnew;

  my @count = $self->{dbh}->selectrow_array("SELECT COUNT(*), COUNT(DISTINCT hash) FROM beacons WHERE seqno==$collno")
      or croak("Could not count for $collno: ".$self->{dbh}->errstr);
  printf("WARNING: expected %u valid records, counted %u\n", $recok, $count[0]) if $recok != $count[0];

  my $updsql =<<"XxX";
UPDATE OR FAIL repos SET counti=?,countu=?,fstat=?,utime=?,ustat=? WHERE seqno==$collno;
XxX
  my $updh = $self->{dbh}->prepare($updsql) or croak("Could not prepare $updsql: ".$self->{dbh}->errstr);
  $updh->execute($count[0], $count[1], $statline, time(), "successfully loaded") or croak("Could not execute $updsql: ".$updh->errstr);

  close(BKN);
  return ($collno, $recok, undef);
}


=head3 processbeaconheader()

Internal subroutine used by loadFile.

=cut

sub processbeaconheader {
  my ($self, $fieldref, %options) = @_;
  my $osq = 0;
  my @carp;

  if ( my $alias = $fieldref->{_alias} ) {
      my $stampfield = SeeAlso::Source::BeaconAggregator->beaconfields("TIMESTAMP");
      my $listsql = "SELECT seqno, $stampfield, mtime, counti FROM repos WHERE alias=?;";
      my $listh = $self->{dbh}->prepare($listsql) or croak("Could not prepare $listsql: ".$self->{dbh}->errstr);
      $listh->execute($alias) or croak("Could not execute $listsql: ".$listh->errstr);
      while ( my($row) = $listh->fetchrow_arrayref ) {
          last unless defined $row;
          if ( $options{'verbose'} ) {
              print "* Old Instances for $alias:\n" unless $osq;
              $osq = $row->[0];
              print "+\t#$osq ", SeeAlso::Source::BeaconAggregator::tToISO($row->[1] || $row->[2]), " (", $row->[3] || "???", ")\n";
            }
          else {
              $osq = $row->[0]};
        }
    };

  my $format = "";
  if ( $fieldref->{'FORMAT'} && $self->{accept}->{'FORMAT'} ) {
      if (  $fieldref->{'FORMAT'} =~ $self->{accept}->{'FORMAT'} ) {
          $format = $fieldref->{'FORMAT'}}
      else {
          push(@carp, "ERROR: only FORMAT '".$self->{accept}->{'FORMAT'}."' are supported")}
    }
  elsif ( $fieldref->{'FORMAT'} ) {
      $format = $fieldref->{'FORMAT'}}
  else {
      push(@carp, "ERROR: header field #FORMAT is mandatory")};

  unless ( $fieldref->{'VERSION'} ) {
       $fieldref->{'VERSION'} ||= "0.1";
       push(@carp, "NOTICE: added header field #VERSION as '".$fieldref->{'VERSION'}."'");
    };
  if ( $self->{accept}->{'VERSION'} ) {
      ($fieldref->{'VERSION'} =~ $self->{accept}->{'VERSION'})
       || push(@carp, "ERROR: only VERSION '".$self->{accept}->{'VERSION'}."' is supported");
    };

  if ( $fieldref->{'ALTTARGET'} ) {
      $fieldref->{'ALTTARGET'} = "" unless defined $fieldref->{'ALTTARGET'};
      my $parsed = hDecode($fieldref, 'ALTTARGET');
      if ( $parsed && ($parsed =~ /(^|[^%])(%.)*%\d\$s/) ) {
          $fieldref->{'ALTTARGET'} = $parsed;
          $format =~ s/\s*-altTARGET//;
          $format .= " -altTARGET";
          ($parsed =~ /(^|[^%])(%.)*%2\$s/) or 
              push(@carp, "WARNING: header field #ALTTARGET should contain placeholder {ALTID}");
        }
      elsif ( $parsed ) {
          push(@carp, "ERROR: header field #ALTTARGET must contain placeholder {ALTID} (or {ID})");
          delete $fieldref->{'ALTTARGET'};
        }
      else {
          push(@carp, "ERROR: could not parse header field #ALTTARGET: '".$fieldref->{'ALTTARGET'}."'");
          delete $fieldref->{'ALTTARGET'};
        }
    };

  if ( $fieldref->{'IMGTARGET'} ) {
      $fieldref->{'IMGTARGET'} = "" unless defined $fieldref->{'IMGTARGET'};
      my $parsed = hDecode($fieldref, 'IMGTARGET');
      if ( $parsed && ($parsed =~ /(^|[^%])(%.)*%\d\$s/) ) {
          $fieldref->{'IMGTARGET'} = $parsed;
          $format =~ s/\s*-imgTARGET//;
          $format .= " -imgTARGET";
        }
      elsif ( $parsed ) {
          push(@carp, "WARNING: header field #IMGTARGET should contain placeholders {ID} or {ALTID}")}
      else {
          push(@carp, "ERROR: could not parse header field #IMGTARGET: '".$fieldref->{'IMGTARGET'}."'");
          delete $fieldref->{'IMGTARGET'};
        }
    };

  if ( exists $fieldref->{'TARGET'} ) {
      $fieldref->{'TARGET'} = "" unless defined $fieldref->{'TARGET'};
      my $parsed = hDecode($fieldref, 'TARGET');
      if ( $parsed && ($parsed =~ /(^|[^%])(%.)*%1\$s/) && ($parsed !~ /(^|[^%])(%.)*%[2-9]\$s/) ) {
          $fieldref->{'TARGET'} = $parsed}
      elsif ( $parsed ) {
          push(@carp, "ERROR: header field #TARGET must contain placeholder {ID} and not {ALTID}");
          delete $fieldref->{'TARGET'};
        }
      else {
          push(@carp, "ERROR: could not parse header field #TARGET: '".$fieldref->{'TARGET'}."'");
          delete $fieldref->{'TARGET'};
        }
    }
  elsif ( $format =~ /^BEACON/ ) {
      push(@carp, "WARNING: header field #TARGET not set: ALL beacon lines will have to provide their link by other means!");
      $format =~ s/\s*-noTARGET//;
      $format .= " -noTARGET";
    }
  else {
      push(@carp, "ERROR: header field #TARGET is mandatory")};


  $fieldref->{'MESSAGE'} = hDecode($fieldref, 'MESSAGE') if $fieldref->{'MESSAGE'};

  if ( $fieldref->{'TIMESTAMP'} ) {
      if ( my $parsed = hDecode($fieldref, 'TIMESTAMP') ) {
          printf("* %-30s %s\n", "Beacon Timestamp:", hEncode($parsed, 'TIMESTAMP')) if $options{'verbose'};
          $fieldref->{'TIMESTAMP'} = $parsed;
        }
      else {           # unparseable => use current
          push(@carp, "WARNING: cannot parse TIMESTAMP '".$fieldref->{'TIMESTAMP'}."', using current time");
          $fieldref->{'TIMESTAMP'} = $^T;
        };
    }
  else {
#     $fieldref->{'TIMESTAMP'} = $fieldref->{'_mtime'} || $^T;
      push(@carp, "NOTICE: no header field #TIMESTAMP detected");
    };

  if ( $fieldref->{'REVISIT'} ) {
      if ( my $parsed = hDecode($fieldref, 'REVISIT') ) {
          if ( $parsed < $^T ) {
              printf("* %-30s %s [%s]\n", "STALE Revisit hint parsed as", hEncode($parsed, 'REVISIT'), $fieldref->{'REVISIT'})}       #  if $options{'verbose'}
          else {
              printf("* %-30s %s\n", "Revisit hint parsed as", hEncode($parsed, 'REVISIT')) if $options{'verbose'}};
          $fieldref->{'REVISIT'} = $parsed;
        }
      else {           # unparseable => discard
          push(@carp, "WARNING: cannot parse #REVISIT '".$fieldref->{'REVISIT'}."', discarding");
          delete $fieldref->{'REVISIT'};
        };
    }
  else {
      push(@carp, "INFO: no header field #REVISIT detected");
    };

  my $cancontinue = 1;
  my $err = "";
  foreach ( @carp ) {
      print "$_\n";
      if ( s/^ERROR: // ) {
          $cancontinue = 0;
          $err .= " | " if $err;
          $err .= $_;
        };
    }
  unless ( $cancontinue or $options{'ignore-header-errors'} ) {
      print "CRITICAL: Aborting because of Header Errors\n";
      return (undef, $err, $format);
    };

  $fieldref->{'_uri'} ||= $fieldref->{'FEED'};
  delete $fieldref->{'_uri'} unless $fieldref->{'_uri'};

  $fieldref->{'_alias'} ||= $fieldref->{'FEED'} || $fieldref->{'TARGET'};

  my (@fn, @fd);
  while ( my ($key, $val) = each %$fieldref ) {
      my $dbkey = "";
      if ( $dbkey = SeeAlso::Source::BeaconAggregator->beaconfields($key) ) {
          push(@fn, $dbkey)}
      elsif ( $key =~ /_(\w+)$/ ) {
          push(@fn, $1)}
      else {
          next};
      my $myval = $val;
      unless ( $myval =~ /^\d*$/ ) {
          $myval =~ s/'/''/g;
          $myval = "'$val'";
        };
      push(@fd, $myval);
    };
  local($") = ",\n";
  my $sql =<<"XxX";
INSERT INTO repos ( seqno, @fn ) VALUES ( NULL, @fd );
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute() or croak("Could not execute $sql:".$sth->errstr);
  my $collno = $self->{dbh}->last_insert_id("", "", "", "");

  my $rhandle;
  if ( $osq ) {
      my $replace =<<"XxX";
UPDATE OR FAIL beacons SET seqno=$collno, hits=?, info=?, link=? WHERE hash=? AND seqno==$osq AND altid=?;
XxX
      $rhandle = $self->{dbh}->prepare($replace) or croak("Could not prepare $replace: ".$self->{dbh}->errstr);
    };
  my $insert =<<"XxX";
INSERT OR IGNORE INTO beacons ( hash, seqno, altid, hits, info, link ) VALUES (?, $collno, ?, ?, ?, ?);
XxX
  my $ihandle = $self->{dbh}->prepare($insert) or croak("Could not prepare $insert: ".$self->{dbh}->errstr);
  return ($collno, "", $format, $ihandle, $rhandle, $osq);
}

=head3 headerfield ( $sq_or_alias, $key [, $value] )

Gets or sets an meta or admin Entry for the constituent file indicated by $sq_or_alias

=cut

sub headerfield {
  my ($self, $sq_or_alias, $key, $value) = @_;

  my $dbkey = "";
  if ( $dbkey = SeeAlso::Source::BeaconAggregator->beaconfields($key) ) {
    }
  elsif ( $key =~ /_(\w+)$/ ) {
     $dbkey = $1}
  else {
     carp "Field $key not known";
     return undef;
    };

  my ($cond, @cval) = SeeAlso::Source::BeaconAggregator::mkConstraint($sq_or_alias);

  my $osql = <<"XxX";
SELECT $dbkey FROM repos $cond;
XxX
  my $osth = $self->{dbh}->prepare($osql) or croak("Could not prepare $osql: ".$self->{dbh}->errstr);
  $osth->execute(@cval) or croak("Could not execute $osql:".$osth->errstr);
  my $tmpval = $osth->fetchall_arrayref();
  my @oval = map { hEncode($_, $key) } map { (defined $_->[0]) ? ($_->[0]) : () } @$tmpval;
  my $rows = scalar @oval;

  if ( $value ) {                # set
      my $usql = <<"XxX";
UPDATE OR FAIL repos SET $dbkey=? $cond;
XxX
      $value = hDecode($value, $key) || "";
      my $usth = $self->{dbh}->prepare($usql) or croak("Could not prepare $usql: ".$self->{dbh}->errstr);
      $rows = $usth->execute($value, @cval) or croak("Could not execute $usql:".$usth->errstr);
    }
  elsif ( defined $value ) {     # clear
      my $dsql = <<"XxX";
UPDATE OR FAIL repos SET $dbkey=? $cond;
XxX
      my $dsth = $self->{dbh}->prepare($dsql) or croak("Could not prepare $dsql: ".$self->{dbh}->errstr);
      $rows = $dsth->execute(undef, @cval) or croak("Could not execute $dsql:".$dsth->errstr);
    }
  else {                         # read
    }

  return ($rows, @oval);
}



my ($lwpcarp817, $lwpcarp827);
=head3 update ($sq_or_alias, $params, %options)

=over 8

=item $sq_or_alias

Sequence number or alias: Used to determine an existing instance

=item $params

Hashref, containing

  agent => LWP::UserAgent to use
  _uri  => Feed URL to load from

=item %options

 verbose => (0|1)
 force => (0|1)

=back

Incorporates a new beacon source from a URI in the database or updates an existing one. 
For HTTP URIs care is taken not to reload an unmodified BEACON feed (unless the 'force'
option is provided). 

If the feed appears to be newer than the previously loaded version it is fetched, 
some UTF-8 adjustments are performed if necessary, then it is stored to a temporary file
and from there finally processed by the loadFile method above.

The URI to load is determined by the following order of precedence:

=over 8

=item 1

_uri Option

=item 2

admin field uri stored in the database

=item 3

meta field #FEED taken from the database

=back

Typical use is with an alias, not with a sequence number:

 $db->update('whatever');

=cut

sub update {
  my ($self, $sq_or_alias, $params, %options) = @_;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};

  my $ua = $params->{'agent'};
  unless ( $ua ) {
      require LWP::UserAgent;
      $ua = LWP::UserAgent->new(agent => "SA-S-BeaconAggregator ",      # end with space to get default agent appended
                            env_proxy => 1,
                              timeout => 300,
                               );
    };

  my ($cond, @cval) = SeeAlso::Source::BeaconAggregator::mkConstraint($sq_or_alias);
  my $alias = ($sq_or_alias =~ /^\d+$/) ? "" : $sq_or_alias;
  my $feedname = SeeAlso::Source::BeaconAggregator->beaconfields("FEED");
  my $ssql = <<"XxX";
SELECT seqno, uri, alias, $feedname, ftime, mtime FROM repos $cond;
XxX
  my $ssth = $self->{dbh}->prepare($ssql) or croak("Could not prepare $ssql: ".$self->{dbh}->errstr);
  $ssth->execute(@cval) or croak("Could not execute $ssql: ".$ssth->errstr);
  my ($osq, $ouri, $oalias, $feed, $fetchtime, $modtime) = $ssth->fetchrow_array;
  croak("Select old instance error: ".$ssth->errstr) if $ssth->err;

  my $uri = $params->{'_uri'} || $ouri || $feed;
  croak("Cannot update $sq_or_alias: No URI given and also not to be determined") unless $uri;
  $uri =~ s/\s$//;
  $alias ||= $oalias || "";

  my $rq = HTTP::Request->new('GET', $uri) or croak("could not construct request from $uri");
  if ( $fetchtime && $modtime  && !$options{'force'} ) {   # allow force-reload by deleting _ftime or _mtime
      printf("  %-30s %s\n", "Old instance stamped", scalar localtime($modtime)) if $options{'verbose'};
      $rq->header('If-Modified-Since', HTTP::Date::time2str($modtime));
    };
  if ( $rq->can("accept_decodable") ) {  # LWP 5.817 and newer
      $rq->accept_decodable}
  else {
      carp("please upgrade to LWP >= 5.817 for compression negotiation") if $options{'verbose'} && (!$lwpcarp817++)};

  my $response = $ua->request($rq);   # Well, we hoggishly slurp everything into memory,
                                      # however explicit decompression of an already dumped result would be PITA
  my $nuri = ($response->request)->uri;
  print "NOTICE: Differing result URI: $nuri\n" if $uri ne $nuri;
  if ( $response->is_success ) {
      print $osq ? "INFO: refreshing $alias sq $osq from $uri\n"
                 : "INFO: importing previously unseen $alias from $uri\n";

      my $charset;
      if ( $response->can("content_charset") ) {    # LWP 5.827 and above
          $charset = $response->content_charset;
          print "DEBUG: Content charset is $charset\n" if $charset && $options{'verbose'};
        }
      else {
          carp("please upgrade to LWP >= 5.827 for better detection of content_charset") if $options{'verbose'} && (!$lwpcarp827++)};
      $charset ||= "UTF-8";

      my $lm = $response->last_modified;
      printf("  %-30s %s\n", "Last modified", scalar localtime($lm)) if $lm && $options{'verbose'};
      $lm ||= $^T;

      my $vt = $response->fresh_until(h_min => 1800, h_max => 30 * 86400);
      printf("  %-30s %s\n", "Should be valid until", scalar localtime($vt)) if $vt && $options{'verbose'};
      $vt ||= 0;

      # temporary file for dumped contents
      my ($tmpfh, $tmpfile) = File::Temp::tempfile("BeaconAggregator-XXXXXXXX", SUFFIX => ".txt", TMPDIR => 1) or croak("Could not acquire temporary file for storage");
      my $contref;   # reference to content buffer
      if ( $response->can("decoded_content") ) {
          $contref = $response->decoded_content( raise_error => 1, ref => 1);
        }
      else {
          $contref = $response->content_ref;
          carp("please upgrade to LWP >= 5.817 for compression handling") if $options{'verbose'} && (!$lwpcarp817++);
        };

      if ( $$contref =~ /^\x{FFEF}/ ) {          # properly encoded BOM => put Characters to file
          binmode($tmpfh, ":utf8");
          print "INFO: properly encoded BOM detected: Groked UTF8\n"; # if $options{'verbose'};
        }
      elsif ( $$contref =~ s/^\xef\xbb\xbf// ) {   # BOM Bytes => put Bytes to file, re-read as UTF-8
          print "INFO: Byte coded BOM detected: trying to restitute character semantics\n"; # if $options{'verbose'};
          print "INFO: Length is ", length($$contref), " ", (utf8::is_utf8($$contref) ? "characters" : "bytes"), "\n";
          binmode($tmpfh, ":bytes");
          if ( utf8::downgrade($$contref, 1) ) {   # 1 = FAIL_OK
              my $prevlength = length($$contref);
              print "INFO: Downgrade was possible, length now $prevlength ", (utf8::is_utf8($$contref) ? "characters" : "bytes"), "\n";
              while ( utf8::decode($$contref) ) {
                  binmode($tmpfh, ":utf8");
                  my $newlength = length($$contref);
                  print "DEBUG: Reassembling as UTF-8 succeeded, length now $newlength ", (utf8::is_utf8($$contref) ? "characters" : "bytes"), "\n";
                  last if $newlength == $prevlength;
                  $prevlength = $newlength;
                };
            }
          else {
              print "WARNING: no downgrade possible, proceed with byte semantics"};
        }
      elsif ( utf8::is_utf8($$contref) ) {       # already Upgraded strings should be written as utf-8
          print "INFO: UTF8-ness already established\n" if $options{'verbose'};
          binmode($tmpfh, ":utf8");
        }
      elsif ( utf8::decode($$contref) ) {        # everything in character semantics now
          print "INFO: Could decode bytes to UTF8-characters\n" if $options{'verbose'};
          binmode($tmpfh, ":utf8");
        }
      else {                                     # leave it alone
          print "WARNING: No clue about character encoding: Assume ISO 8859-1\n"; # if $options{'verbose'};
          binmode($tmpfh, ":utf8");
        };
      print $tmpfh $$contref;
      close($tmpfh);

      my ($collno, $count, $statref) = $self->loadFile($tmpfile, {_alias => $alias, _uri => $uri, _ruri => $nuri, _mtime => $lm}, %options);
      if ( ! $collno && $osq ) {
          my $usql = <<"XxX";
UPDATE OR FAIL repos SET utime=?,ustat=? WHERE seqno==$osq;
XxX
          my $usth = $self->{dbh}->prepare($usql) or croak("Could not prepare $usql: ".$self->{dbh}->errstr);
          $usth->execute(time(), $statref ? "load error: $statref" : "internal error") or croak("Could not execute $usql: ".$usth->errstr);
        };

      unlink($tmpfile) if -f $tmpfile;
      return $collno ? ($collno, $count) : undef;
    }
  elsif ( $response->code == 304 ) {
      print "INFO: $alias not modified since ".HTTP::Date::time2str($modtime)."\n";
      my $vt = $response->fresh_until(h_min => 1800, h_max => 6 * 86400);
      printf("  %-30s %s\n", "Will not try again before", scalar localtime($vt)) if $options{'verbose'};
      my $usql = <<"XxX";
UPDATE OR FAIL repos SET utime=?,ustat=?,ruri=? WHERE seqno==$osq;
XxX
      my $usth = $self->{dbh}->prepare($usql) or croak("Could not prepare $usql: ".$self->{dbh}->errstr);
      $usth->execute(time(), $response->status_line, $nuri) or croak("Could not execute $usql: ".$usth->errstr);
      return undef;
    }
  else {
      print "WARNING: No access to $uri for $alias [".$response->status_line."]\n";
      print $response->headers_as_string, "\n";
      return undef unless $osq;
      my $usql = <<"XxX";
UPDATE OR FAIL repos SET utime=?,ustat=?,ruri=? WHERE seqno==$osq;
XxX
      my $usth = $self->{dbh}->prepare($usql) or croak("Could not prepare $usql: ".$self->{dbh}->errstr);
      $usth->execute(time(), $response->status_line, $nuri) or croak("Could not execute $usql: ".$usth->errstr);
      return undef;
    };
}

=head3 listCollections ( [ $seqno_or_alias ] )

=cut

sub listCollections {
  my ($self, $seqno_or_alias) = @_;

  unless ( $self->{_iterator_listCollections} ) {
      my ($constraint, @cval) = SeeAlso::Source::BeaconAggregator::mkConstraint($seqno_or_alias);
      my ($sql) =<<"XxX";
SELECT seqno, alias, uri, mtime, counti, countu FROM repos $constraint;
XxX
      my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
      $sth->execute(@cval) or croak("Could not execute $sql: ".$sth->errstr);
      $self->{_iterator_listCollections} = $sth;
    };
  my @onerow = $self->{_iterator_listCollections}->fetchrow_array;
  unless ( @onerow ) {
      croak("Error listing Collections: $self->{_iterator_listCollections}->errstr") if $self->{_iterator_listCollections}->err;
      delete $self->{_iterator_listCollections};
    };
  return @onerow;
}

=head3 headers ( [ $seqno_or_alias ] ) 

=cut

sub headers {
  my ($self, $seqno_or_alias) = @_;

  unless ( $self->{_iterator_info} ) {
      my ($constraint,  @cval) = SeeAlso::Source::BeaconAggregator::mkConstraint($seqno_or_alias);
      my ($sql) =<<"XxX";
SELECT * FROM repos $constraint;
XxX
      my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
      $sth->execute(@cval) or croak("Could not execute $sql: ".$sth->errstr);
      $self->{_iterator_info} = $sth;
    };

  my $info = $self->{_iterator_info}->fetchrow_hashref;
  unless ( defined $info ) {
      croak("Error listing Collections: $self->{_iterator_info}->errstr") if $self->{_iterator_info}->err;
      delete $self->{_iterator_info};
      return undef;
    }

  my $collno = $info->{seqno} || $seqno_or_alias;
  my @livecounts = $self->{dbh}->selectrow_array("SELECT COUNT(*), COUNT(DISTINCT hash) FROM beacons WHERE seqno==$collno")
      or croak("Could not count for $collno: ".$self->{dbh}->errstr);
  my %meta = ('-live_count_id' => $livecounts[0] || 0, '-live_unique_id' => $livecounts[1] || 0, _seqno => $collno);
  my %result = ();
  while ( my($key, $val) = each %$info ) {
      my $pval = hEncode($val, $key);

      if ( $key =~ /^bc(\w+)$/ ) {
          $result{$1} = $pval}
      else {
          $meta{"_$key"} = $pval};
    }
  return \%result, \%meta;
}

=head3 unload ( [ $seqno_or_alias, %options ] ) 

=cut

sub unload {
  my ($self, $seqno_or_alias, %options) = @_;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};

  my @seqnos = ();
  if ( $seqno_or_alias && ($seqno_or_alias =~ /^\d+$/) ) {
      @seqnos = ($seqno_or_alias)}
  elsif ( $seqno_or_alias || $options{'force'} ) {
      @seqnos = $self->Seqnos('_alias', $seqno_or_alias ? ($seqno_or_alias) : ());
      unless ( @seqnos ) {
          carp("no Seqnos selected by $seqno_or_alias");
          return 0;
        };
      unless ( $options{'force'} or (@seqnos == 1) ) {
          carp("Use --force to purge more than one sequence (@seqnos)");
          return 0;
        };
    }
  else {
      carp("Use --force to purge the complete database");
      return 0;
    };

  if ( $options{'force'} ) {
      my ($sql) =<<"XxX";
DELETE FROM beacons WHERE seqno==?;
XxX
      my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
      foreach my $seqno ( @seqnos ) {
          my $rows = $sth->execute($seqno_or_alias) or croak("Could not execute $sql: ".$sth->errstr);
          print "INFO: $rows forced for $seqno\n" if $options{'verbose'};
        };
    };

  my ($cond, @cval) = SeeAlso::Source::BeaconAggregator::mkConstraint($seqno_or_alias);
  my ($sql) =<<"XxX";
DELETE FROM repos $cond;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  my $rows = $sth->execute(@cval) or croak("Could not execute $sql: ".$sth->errstr);
  $rows = 0 if $rows eq "0E0";
  return $rows;
}


=head3 purge ( $seqno_or_alias[, %options ] ) 

=cut

sub purge {
  my ($self, $seqno_or_alias, %options) = @_;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};
  my @seqnos;
  if ( $seqno_or_alias && ($seqno_or_alias =~ /^\d+$/) ) {
      @seqnos = ($seqno_or_alias)}
  elsif ( $seqno_or_alias || $options{'force'} ) {
      @seqnos = $self->Seqnos('_alias', $seqno_or_alias ? ($seqno_or_alias) : ());
      unless ( @seqnos ) {
          carp("no Seqnos selected by $seqno_or_alias");
          return 0;
        };
      unless ( $options{'force'} or (@seqnos == 1) ) {
          carp("Use --force to purge more than one sequence (@seqnos)");
          return 0;
        };
    }
  else {
      carp("Use --force to purge the complete database");
      return 0;
    };
  my ($sql) =<<"XxX";
DELETE FROM beacons WHERE seqno==?;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  my ($usql) =<<"XxX";
UPDATE OR FAIL repos SET counti=?,countu=?,utime=?,ustat=? WHERE seqno==?;
XxX
  my $usth = $self->{dbh}->prepare($usql) or croak("Could not prepare $usql: ".$self->{dbh}->errstr);
  my $trows = 0;
  foreach my $seqno ( @seqnos ) {
      my $rows = $sth->execute($seqno) or croak("Could not execute $sql: ".$sth->errstr);
      $rows = "0" if $rows eq "0E0";
      print "INFO: $rows purged for $seqno\n" if $options{'verbose'};
      $trows += $rows;
      $usth->execute(0, 0, time, "purged", $seqno) or croak("Could not execute $usql: ".$usth->errstr);
    };
  return $trows;
}

=head3 idStat ( [ $seqno_or_alias, %options ] ) 

=cut

sub idStat {
  my ($self, $seqno_or_alias, %options) = @_;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};
  my $cond = "";
  if ( $seqno_or_alias && ($seqno_or_alias =~ /^\d+$/) ) {
      $cond = "WHERE seqno==$seqno_or_alias"}
  elsif ( $seqno_or_alias ) {
      my @seqnos = $self->Seqnos('_alias', $seqno_or_alias);
      if ( @seqnos ) {
          $cond = "WHERE seqno IN (".join(",", @seqnos).")"}
      else {
          carp("no Seqnos selected by $seqno_or_alias");
          return 0;
        };
    };
  my $count_what = $options{'distinct'} ? "DISTINCT hash" : "hash";
  my ($sql) =<<"XxX";
SELECT COUNT($count_what) FROM beacons $cond;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute() or croak("Could not execute $sql: ".$sth->errstr);
  my ($hits) = $sth->fetchrow_array || (0);
  return $hits;
};

=head3 idCounts ( [ $seqno_or_alias, %options ] ) 

=cut

sub idCounts {
  my ($self, $pattern, %options) = @_;
  my $cond = $pattern ? qq!WHERE hash LIKE "$pattern"! : "";
  my $count_what = $options{'distinct'} ? "DISTINCT hash" : "hash";
  unless ( $self->{_iterator_idCounts} ) {
      my ($sql) =<<"XxX";
SELECT hash, COUNT($count_what), SUM(hits) FROM beacons $cond GROUP BY hash ORDER BY hash;
XxX
      my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
      $sth->execute() or croak("Could not execute $sql: ".$sth->errstr);
      $self->{_iterator_idCounts} = $sth;
    };
  my @onerow = $self->{_iterator_idCounts}->fetchrow_array;
  unless ( @onerow ) {
      croak("Error listing Collections: $self->{_iterator_idCounts}->errstr") if $self->{_iterator_idCounts}->err;
      delete $self->{_iterator_idCounts};
      return ();
    };
  if ( defined $self->{identifierClass} ) {
      my $c = $self->{identifierClass};
      $c->hash($onerow[0]);
      $onerow[0] = $c->can("pretty") ? $c->pretty() : $c->value();
    };
  return @onerow;
};

=head3 idList ( [ $seqno_or_alias, %options ] ) 

=cut

sub idList {
  my ($self, $pattern) = @_;
  my $cond = $pattern ? qq!WHERE hash LIKE "$pattern"! : "";
  unless ( $self->{_iterator_idList_handle} ) {
      my ($sql) =<<"XxX";
SELECT hash, seqno, hits, info, link FROM beacons $cond ORDER BY hash, seqno, altid;
XxX
      my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
      $sth->execute() or croak("Could not execute $sql: ".$sth->errstr);
      $self->{_iterator_idList_handle} = $sth;
      $self->{_iterator_idList_crosscheck} = $self->RepoCols("ALTTARGET");
      $self->{_iterator_idList_prefetch} = undef;
    };
  unless ( exists $self->{_iterator_idList_prefetch} ) {   # deferred exit
      delete $self->{_iterator_idList_handle};
      delete $self->{_iterator_idList_crosscheck};
      return ();
    };
  my $pf = $self->{_iterator_idList_prefetch};
  while ( my $onerow = $self->{_iterator_idList_handle}->fetchrow_arrayref ) {
      $onerow->[2] = "" unless $self->{_iterator_idList_crosscheck}->{$onerow->[1]};  # kill artefacts
      if ( defined $self->{identifierClass} ) {
          my $c = $self->{identifierClass};
          $c->hash($onerow->[0]);
          $onerow->[0] = $c->can("pretty") ? $c->pretty() : $c->value();
        };
      if ( $pf ) {
          if ( $pf->[0] eq $onerow->[0] ) {
              push(@$pf, [@$onerow[1..@$onerow-1]]);
              next;
            }
          else {
              $self->{_iterator_idList_prefetch} = [$onerow->[0], [@$onerow[1..@$onerow-1]]];
              return @$pf;
            }
         }
       else {
           $pf = [$onerow->[0], [@$onerow[1..@$onerow-1]]]};
    };
  
  if ( $self->{_iterator_idList_handle}->err ) {
      croak("Error listing Collections: $self->{_iterator_idList_handle}->errstr");
    };
  delete $self->{_iterator_idList_prefetch};
  return $pf ? @$pf : ();
};


=head3 setOSD ( $field, $value }

Sets the field $field of the OpenSearchDescription to $value.

=cut

sub setOSD {
  my ($self) = shift;
  $self->clearOSD(@_) or return undef;
  return $self->addOSD(@_);
};

=head3 setOSD ( $field }

Clears the field $field of the OpenSearchDescription.

=cut

sub clearOSD {
  my ($self, $field) = @_;
  $field || (carp("no OSD field name provided"), return undef);
  defined $self->osdKeys($field) || (carp("no valid OSD field '$field'"), return undef);
  my $sql = <<"XxX";
DELETE FROM osd WHERE key=?;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute($field) or croak("Could not execute $sql: ".$sth->errstr);
  return 1;
}

=head3 addOSD ( $field, $value }

Appends $value the (repeatable) field $field of the OpenSearchDescription.

=cut

sub addOSD {
  my ($self, $field, $value) = @_;
  $field || (carp("no OSD field name provided"), return undef);
  defined $self->osdKeys($field) || (carp("no valid OSD field '$field'"), return undef);
  my $sql = <<"XxX";
INSERT INTO osd ( key, val ) VALUES ( ?, ? );
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute($field, $value) or croak("Could not execute $sql: ".$sth->errstr);
  return 1;
}


=head3 setBeaconMeta ( $field, $value )

Sets the field $field of the Beacon meta table (used to generate a BEACON file for this
service) to $value.

=cut

sub setBeaconMeta {
  my ($self) = shift;
  $self->clearBeaconMeta(@_) or return undef;
  return $self->addBeaconMeta(@_) if defined $_[1];        # value to set
};

=head3 clearBeaconMeta ( $field }

Deletes the field $field of the Beacon meta table.

=cut

sub clearBeaconMeta {
  my ($self, $field) = @_;
  $field || (carp("no Beacon field name provided"), return undef);
  $field = $self->beaconfields($field) or (carp("no valid Beacon field '$field'"), return undef);
  my $sql = <<"XxX";
DELETE FROM osd WHERE key=?;
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute($field) or croak("Could not execute $sql: ".$sth->errstr);
  return 1;
}

=head3 addBeaconMeta ( $field, $value )

Appends $value to the field $field of the BEACON meta table

=cut
sub addBeaconMeta {
  my ($self, $field, $value) = @_;
  $field || (carp("no Beacon field name provided"), return undef);
  $field = $self->beaconfields($field) or (carp("no valid Beacon field '$field'"), return undef);
  my $sql = <<"XxX";
INSERT INTO osd ( key, val ) VALUES ( ?, ? );
XxX
  my $sth = $self->{dbh}->prepare($sql) or croak("Could not prepare $sql: ".$self->{dbh}->errstr);
  $sth->execute($field, $value) or croak("Could not execute $sql: ".$sth->errstr);
  return 1;
}

# on-the-fly conversions

sub hDecode {      # external time to numeric timestamp, printf placeholders
  my ($val, $fnam) = @_;
  return $val unless $fnam;
  local($_) = (ref $val) ? $val->{$fnam} : $val;
  return undef unless defined $_;

  if    ( $fnam =~ /target$/i )  { s/%/%%/g; s/(\{id\}|\$PND)/%1\$s/gi; s/(\{altid\}|\$PND)/%2\$s/gi; }
  elsif ( $fnam =~ /message$/i ) { s/%/%%/g; s/\{hits?\}/%s/gi;     }
  elsif ( $fnam =~ /time|revisit/i ) {
      if ( /^\d+$/ ) {     # legacy UNIX timestamp
        }
      elsif ( my $p = HTTP::Date::str2time($_, "GMT") ) {  # all unqualified times are GMT
          $_ = $p}
      else {
          carp("could not parse value '$_' as time in field $fnam");
          return undef;
        };
    }
  return $_;
}

sub hEncode {     # timestamp to beacon format
  my ($val, $fnam) = @_;
  local($_) = (ref $val) ? $val->{$fnam} : $val;
  return undef unless defined $_;
  if    ( $fnam =~ /time|revisit/i ) { $_ = SeeAlso::Source::BeaconAggregator::tToISO($_) }
  elsif ( $fnam =~ /message/i )      { s/%s/{hits}/; s/%%/%/g; }
  elsif ( $fnam =~ /target/i )       { s/%s/{ID}/;   s/%1\$s/{ID}/;   s/%2\$s/{ALTID}/;   s/%%/%/g; };
  return $_;
}

=head1 AUTHOR

    Thomas Berger
    CPAN ID: THB
    gymel.com
    THB@cpan.org

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.

=cut

1;

