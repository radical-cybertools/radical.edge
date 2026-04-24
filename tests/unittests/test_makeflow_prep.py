"""Unit tests for bin/radical-edge-makeflow-prep.

Exercises the preprocessor's parser, directive scoping, rewrite
semantics, and error reporting.  The script has no ``.py`` extension
so we load it via ``SourceFileLoader``.
"""

from importlib.machinery import SourceFileLoader
from pathlib import Path

import pytest


_PREP = SourceFileLoader(
    'prep_mod',
    str(Path(__file__).resolve().parents[2]
        / 'bin' / 'radical-edge-makeflow-prep')
).load_module()

PrepOptions = _PREP.PrepOptions
PrepError   = _PREP.PrepError
prep_stream = _PREP.prep_stream


def _run(text: str, **opts_kwargs) -> str:
    opts = PrepOptions(**opts_kwargs)
    lines = text.splitlines(keepends=True)
    return ''.join(prep_stream(lines, 'runid0', opts))


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestBasicRewrite:

    def test_minimal(self):
        out = _run(
            'EDGE = "e1"\n'
            'POOL = "p1"\n'
            '\n'
            'out.dat: in.dat\n'
            '\t./compute in.dat out.dat\n')
        assert 'radical-edge-run' in out
        assert '--edge=e1' in out
        assert '--pool=p1' in out
        assert '--run-id=runid0' in out
        assert '--in in.dat' in out
        assert '--out out.dat' in out
        assert '-- ./compute in.dat out.dat' in out

    def test_directives_consumed(self):
        out = _run(
            'EDGE = "e1"\n'
            'POOL = "p1"\n'
            'out.dat: in.dat\n\t./foo\n')
        # EDGE/POOL lines are stripped from output
        assert 'EDGE' not in out
        assert 'POOL' not in out

    def test_priority_passed_through(self):
        out = _run(
            'EDGE = "e"\nPOOL = "p"\nPRIORITY = 42\n'
            'o: i\n\tcmd\n')
        assert '--priority=42' in out

    def test_no_priority_defaults_to_zero(self):
        out = _run('EDGE = "e"\nPOOL = "p"\n'
                   'o: i\n\tcmd\n')
        assert '--priority=0' in out


# ---------------------------------------------------------------------------
# Scoping
# ---------------------------------------------------------------------------

class TestScoping:

    def test_scope_applies_to_subsequent_rules(self):
        out = _run(
            'EDGE = "e1"\nPOOL = "p1"\n'
            'a: i1\n\tc1\n'
            'EDGE = "e2"\n'
            'b: i2\n\tc2\n')
        lines = [l for l in out.split('\n') if 'radical-edge-run' in l]
        assert len(lines) == 2
        assert '--edge=e1' in lines[0]
        assert '--edge=e2' in lines[1]
        # pool stays p1 in rule 2 since POOL wasn't re-set
        assert '--pool=p1' in lines[1]

    def test_default_edge_option(self):
        out = _run('POOL = "p"\no: i\n\tcmd\n',
                   default_edge='eD')
        assert '--edge=eD' in out

    def test_default_pool_option(self):
        out = _run('EDGE = "e"\no: i\n\tcmd\n',
                   default_pool='pD')
        assert '--pool=pD' in out

    def test_explicit_overrides_default(self):
        out = _run('EDGE = "eX"\nPOOL = "pX"\n'
                   'o: i\n\tcmd\n',
                   default_edge='eD', default_pool='pD')
        assert '--edge=eX' in out
        assert '--edge=eD' not in out


# ---------------------------------------------------------------------------
# Pass-through
# ---------------------------------------------------------------------------

class TestPassThrough:

    def test_comments_preserved(self):
        src = ('# a comment\nEDGE = "e"\nPOOL = "p"\n'
               '# another\n'
               'o: i\n\tcmd\n# after rule\n')
        out = _run(src)
        assert '# a comment' in out
        assert '# another' in out
        assert '# after rule' in out

    def test_blank_lines_preserved(self):
        src = 'EDGE = "e"\nPOOL = "p"\n\n\no: i\n\tcmd\n\n'
        out = _run(src)
        # Blank lines from directive positions are now blank but the
        # original structural blanks survive.  Count roughly.
        assert out.count('\n\n') >= 2

    def test_unknown_variable_passed_through(self):
        src = ('CATEGORY = "big"\nMEMORY = 16384\n'
               'EDGE = "e"\nPOOL = "p"\no: i\n\tcmd\n')
        out = _run(src)
        assert 'CATEGORY = "big"' in out
        assert 'MEMORY = 16384' in out


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

class TestErrors:

    def test_rule_without_edge(self):
        with pytest.raises(PrepError, match='EDGE'):
            _run('POOL = "p"\no: i\n\tcmd\n')

    def test_rule_without_pool(self):
        with pytest.raises(PrepError, match='POOL'):
            _run('EDGE = "e"\no: i\n\tcmd\n')

    def test_rule_with_no_command(self):
        with pytest.raises(PrepError, match='no command'):
            _run('EDGE = "e"\nPOOL = "p"\no: i\n')

    def test_rule_header_backslash_continuation(self):
        with pytest.raises(PrepError, match='multi-line'):
            _run('EDGE = "e"\nPOOL = "p"\no: i \\\n  j\n\tcmd\n')

    def test_priority_non_integer(self):
        with pytest.raises(PrepError, match='PRIORITY'):
            _run('PRIORITY = not-a-number\nEDGE = "e"\nPOOL = "p"\n'
                 'o: i\n\tcmd\n')


# ---------------------------------------------------------------------------
# Multi-command rules joined with ;
# ---------------------------------------------------------------------------

class TestMultiCommand:

    def test_two_commands_joined(self):
        out = _run(
            'EDGE = "e"\nPOOL = "p"\n'
            'o: i\n'
            '\tpart1\n'
            '\tpart2\n')
        # joined with ' ; '
        assert '-- part1 ; part2' in out


# ---------------------------------------------------------------------------
# run_id derivation
# ---------------------------------------------------------------------------

class TestRunId:

    def test_run_id_deterministic(self, tmp_path: Path):
        p = tmp_path / 'wf.makeflow'
        p.write_text('EDGE = "e"\n')
        r1 = _PREP.compute_run_id(p)
        r2 = _PREP.compute_run_id(p)
        assert r1 == r2

    def test_run_id_changes_on_mtime(self, tmp_path: Path):
        import os, time
        p = tmp_path / 'wf.makeflow'
        p.write_text('EDGE = "e"\n')
        r1 = _PREP.compute_run_id(p)
        time.sleep(0.01)
        os.utime(p, None)   # bumps mtime
        r2 = _PREP.compute_run_id(p)
        assert r1 != r2
