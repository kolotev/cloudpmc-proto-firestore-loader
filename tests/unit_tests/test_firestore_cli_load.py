from click.testing import CliRunner

from cloudpmc_proto_firestore_loader import cli_main


def test_cli_main():
    runner = CliRunner()
    result = runner.invoke(cli_main, ["load", "--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output
    assert "--help" in result.output
    assert "JSON_FILES" in result.output
