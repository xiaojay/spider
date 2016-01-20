<?php

set_time_limit(0);

$genedata = new genedata;
$genedata->run();

class genedata
{
    private $file = './time.txt';
    private $get_i = 0;
    private $mysqli;

    public function __construct() {
        file_exists($this->file) and is_readable($this->file) and is_writable($this->file) or exit('file permission error');

        $this->mysqli = new mysqli('localhost', 'root', 'root', 'genedata') or exit('connect mysql error');
        $this->mysqli->set_charset('utf8');
    }

    public function __destruct() {
        $this->mysqli->close();
    }

    public function run($last_time = null) {
        if (!$last_time)
            $last_time = file_get_contents($this->file) or exit('get last update time error');

        $page_html = $this->get('Special:RecentChanges&from=' . $last_time . '&namespace=0&limit=5000&days=90&hidebots=0');
        if (!$page_html) goto sleep;

        preg_match_all('~<a href="/index.php/(.+)" title=".+" class="mw-changeslist-title">~i', $page_html, $page_matches);
        if (!$page_matches[1]) goto end;

        $title = array();
        foreach ($page_matches[1] AS $page_match) {
            preg_match('~\(.+\)$~i', $page_match, $matches);
            if ($matches[0]) $page_match = substr($page_match, 0, -strlen($matches[0]));

            if (in_array($page_match, $title)) continue;
            $title[] = $page_match;

            unset($snp);

            $snp['time'] = time();

            $snp['rsid'] = strtoupper($page_match);

            $page_match = urlencode($page_match);

            sleep(1);
            $html = $this->get($page_match);
            if (!$html) continue;

            preg_match('~<body[^>]*>(.+)</body>~is', $html, $matches);
            if (!$matches[1]) continue;
            $html = $matches[1];

            preg_match('~<a href="/index.php/Category:Is_a_snp" title="Category:Is a snp">Is a snp</a></li>~', $html, $matches);
            if (!$matches[0]) continue;

            sleep(1);
            $raw = $this->get($page_match . '&action=raw');
            if (!$raw) continue;

            preg_match('~\|Chromosome=(\d+|X|Y)~i', $raw, $matches);
            $snp['chr'] = ($matches[1]) ?: '';

            preg_match('~\|position=(\d+)~i', $raw, $matches);
            $snp['pos'] = ($matches[1]) ?: '0';

            preg_match('~\|Summary=(.+)~i', $raw, $matches);
            $snp['description'] = ($matches[1]) ?: '';

            if (preg_match('~\|Gene_s=(.+)~i', $raw, $matches)) {
                $snp['gene'] = $matches[1];
            } else {
                preg_match('~\|Gene=(.+)~i', $raw, $matches);
                $snp['gene'] = ($matches[1]) ?: '';
            }

            $snp['rawtext'] = $raw;

            preg_match('~\|GMAF=([\d\.]+)~i', $raw, $matches);
            $snp['gmaf'] = ($matches[1]) ?: '0';

            preg_match('~\|Orientation=(minus|plus)~i', $raw, $matches);
            $snp['orientation'] = ($matches[1]) ?: '';

            preg_match_all('~.*\[.*<a[^>]+href="([^"]+)"[^>]*>[^<]+</a>.*\].*~i', $html, $matches);
            if ($matches[0]) {
                foreach ($matches[0] as $key => $value) {
                    if (substr($matches[1][$key], 0, 2) == '//') $matches[1][$key] = 'http:' . $matches[1][$key];
                    $value = strip_tags($value);

                    $snp['reference'][$key] = array(
                        'url' => $matches[1][$key],
                        'content' => $value
                    );
                }

                $snp['reference'] = json_encode($snp['reference']);
            } else {
                $snp['reference'] = null;
            }

            snp:
            $old_snp = $this->select('snps', array('rsid' => $snp['rsid']));

            if ($old_snp) {
                $snpid = $old_snp['snpid'];

                $this->update('snps', $snp, array('snpid' => $snpid));
            }
            else {
                $snpid = $this->insert('snps', $snp);
            }

            if (!$snpid) continue;

            genotype:
            preg_match('~<table class="sortable smwtable" width="100%">(?:(?!<tr>).)*<tr>(?:(?!</tr>).)+</tr>(?:(?!<tr>).)*((?:<tr>(?:(?!</tr>).)+</tr>(?:(?!(?:<tr>|</table>)).)*)+)</table>~is', $html, $matches);
            if (!$matches[1]) goto phenotype;

            preg_match_all('~<tr>[^<]*<td>[^<]*<a[^>]+>\(([^\)]+)\)</a>[^<]*</td>[^<]*<td style="background: #([a-z0-9]+)">([^<]*)</td>[^<]*<td>([^<]*)</td>[^<]*</tr>~i', $matches[1], $matches);
            if (!$matches[0]) goto phenotype;

            foreach ($matches[0] as $key => $value) {
                $matches[1][$key] = str_replace(array('-', ';'), '', $matches[1][$key]);

                unset($geno);

                $geno['time'] = time();

                $geno['snpid'] = $snpid;

                $geno['genotype'] = $matches[1][$key];

                switch ($matches[2][$key]) {
                    case '80ff80':
                        $geno['repute'] = 'good';

                        break;

                    case 'ff8080':
                        $geno['repute'] = 'bad';

                        break;

                    default:
                        $geno['repute'] = '';

                        break;
                }

                $geno['mag'] = ($matches[3][$key]) ?: '';

                $geno['summary'] = ($matches[4][$key]) ?: '';

                $old_geno = $this->select('genotypes', array(
                    'snpid' => $geno['snpid'],
                    'genotype' => $geno['genotype']
                ));

                if ($old_geno) {
                    $genotypeid = $old_geno['genotypeid'];

                    $this->update('snps', $geno, array('genotypeid' => $genotypeid));
                } else {
                    $genotypeid = $this->insert('genotypes', $geno);
                }
            }

            phenotype:
            preg_match_all('~<div style="clear:right; float:right; margin-left:1em; margin-bottom:1em; width:25em; text-align: left; font-size: 90%; border:thin solid;"><table width="100%">(.+?)</table></div>~is', $html, $pheno_matches);

            if (!$pheno_matches[1]) continue;

            foreach ($pheno_matches[1] AS $pheno_match) {
                preg_match('~<a[^>]+>OMIM</a>(?:(?!<a).)+<a(?:(?!href).)+href="(?:http:)?//www.ncbi.nlm.nih.gov/omim/\d+">(\d+)</a>~i', $pheno_match, $matches);
                if (!$matches[1]) continue;

                unset($pheno);

                $pheno['time'] = time();

                $pheno['snpid'] = $snpid;

                $pheno['omim'] = $matches[1];

                preg_match('~<td>Desc</td>[^<]*<td>([^<]+)</td>~i', $pheno_match, $matches);
                $pheno['description'] = ($matches[1]) ?: '';

                preg_match('~<td>Variant</td>[^<]*<td>[^<]*<a(?:(?!href).)+href="(?:http:)?//www.ncbi.nlm.nih.gov/omim/\d+#\d+">(\d+)</a>~i', $pheno_match, $matches);
                $pheno['variant'] = ($matches[1]) ?: '0';

                preg_match('~<td>Related</td>[^<]*<td>[^<]*<a(?:(?!href).)+href="((?:http://|/)?[^"]+)"[^>]*>~i', $pheno_match, $matches);
                if ($matches[1]) {
                    if (substr($matches[1], 0, 1) == '/')
                        $matches[1] = (substr($matches[1], 0, 2) == '//') ? 'http:' . $matches[1] : 'http://snpedia.com' . $matches[1];

                    $pheno['related'] = $matches[1];
                } else {
                    $pheno['related'] = '';
                }

                $old_pheno = $this->select('phenotypes', array(
                    'snpid' => $pheno['snpid'],
                    'omim' => $pheno['omim']
                ));

                if ($old_pheno) {
                    $phenotypeid = $old_pheno['phenotypeid'];

                    $this->update('snps', $pheno, array('phenotypeid' => $phenotypeid));
                } else {
                    $phenotypeid = $this->insert('phenotypes', $pheno);
                }
            }
        }

        end:
        file_put_contents($this->file, date('YmdHis')) or exit('write last update time to file error');

        sleep:
        $this->__destruct();

        sleep(3600);

        $this->__construct();

        return $this->run();
    }

    public function get($query) {
        if ($this->get_i > 10) {
            $this->get_i = 0;
            return false;
        }

        $result = file_get_contents('http://snpedia.com/index.php?title=' . $query);
        if (!$result) {
            $this->get_i++;
            sleep(10);
            return $this->get($query);
        }

        $this->get_i = 0;
        return $result;
    }

    private function insert($table, $data)
    {
        if (!$data || !is_array($data)) return false;

        foreach ($data as $key => $val) {
            $column[] = '`' . $this->mysqli->real_escape_string(trim($key)) . '`';
            $value[] = ($val === null) ? 'null' : '"' . $this->mysqli->real_escape_string(trim($val)) . '"';
        }

        $this->mysqli->query('INSERT INTO `' . $table . '` (' . implode(', ', $column) . ') VALUE (' . implode(', ', $value) . ')');

        return $this->mysqli->insert_id;
    }

    private function update($table, $data, $where)
    {
        if (!$data || !is_array($data) || !$where || !is_array($where)) return false;

        foreach ($data as $key => $val) {
            if ($val === null)
                $update_data[] = '`' . $this->mysqli->real_escape_string(trim($key)) . '` = null';
            else
                $update_data[] = '`' . $this->mysqli->real_escape_string(trim($key)) . '` = "' . $this->mysqli->real_escape_string(trim($val)) . '"';
        }

        foreach ($where as $key => $val) {
            if ($val === null)
                $where_data[] = '`' . $this->mysqli->real_escape_string(trim($key)) . '` = null';
            else
                $where_data[] = '`' . $this->mysqli->real_escape_string(trim($key)) . '` = "' . $this->mysqli->real_escape_string(trim($val)) . '"';
        }

        $this->mysqli->query('UPDATE `' . $table . '` SET ' . implode(', ', $update_data) . ' WHERE ' . implode(' AND ', $where_data));

        return $this->mysqli->affected_rows;
    }

    private function select($table, $where, $rows = false)
    {
        if (!$where || !is_array($where)) return false;

        foreach ($where as $key => $val) {
            if ($val === null)
                $where_data[] = '`' . $this->mysqli->real_escape_string(trim($key)) . '` = null';
            else
                $where_data[] = '`' . $this->mysqli->real_escape_string(trim($key)) . '` = "' . $this->mysqli->real_escape_string(trim($val)) . '"';
        }

        $result = $this->mysqli->query('SELECT * FROM `' . $table . '` WHERE ' . implode(' AND ', $where_data));

        if ($rows) {
            $data = array();

            while ($row = $result->fetch_assoc()) $data[] = $row;
        } else {
            $data = $result->fetch_assoc();
        }

        $result->free();

        return $data;
    }
}
