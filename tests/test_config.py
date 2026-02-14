"""Tests for configuration system (Phase 2, Steps 2.1-2.8)."""

import pytest

from sql_tool.cli.main import app
from sql_tool.core.config import (
    AppConfig,
    PgProfile,
    ResolvedConfig,
    load_config,
    parse_dsn,
    resolve_config,
)
from sql_tool.core.exceptions import ConfigError

# ---------------------------------------------------------------------------
# Step 2.1: Configuration Models
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestParseDsn:
    def test_full_dsn(self):
        result = parse_dsn(
            "postgresql://myuser:mypass@dbhost:5433/mydb?sslmode=require"
        )
        assert result["host"] == "dbhost"
        assert result["port"] == 5433
        assert result["dbname"] == "mydb"
        assert result["user"] == "myuser"
        assert result["password"] == "mypass"
        assert result["sslmode"] == "require"

    def test_minimal_dsn(self):
        result = parse_dsn("postgresql://localhost/testdb")
        assert result["host"] == "localhost"
        assert result["dbname"] == "testdb"
        assert "port" not in result
        assert "user" not in result

    def test_postgres_scheme(self):
        result = parse_dsn("postgres://localhost/testdb")
        assert result["host"] == "localhost"

    def test_invalid_scheme(self):
        with pytest.raises(ConfigError, match="Invalid DSN scheme"):
            parse_dsn("mysql://localhost/testdb")

    def test_dsn_with_connect_timeout(self):
        result = parse_dsn("postgresql://localhost/db?connect_timeout=5")
        assert result["connect_timeout"] == 5

    def test_dsn_with_application_name(self):
        result = parse_dsn("postgresql://localhost/db?application_name=myapp")
        assert result["application_name"] == "myapp"


@pytest.mark.unit
class TestPgProfile:
    def test_default_values(self):
        profile = PgProfile()
        assert profile.host == "localhost"
        assert profile.port == 5432
        assert profile.dbname == "postgres"
        assert profile.user is None
        assert profile.password is None
        assert profile.sslmode == "prefer"
        assert profile.connect_timeout == 10
        assert profile.application_name == "sql-tool"

    def test_from_individual_fields(self):
        profile = PgProfile(host="dbhost", port=5433, dbname="mydb", user="admin")
        assert profile.host == "dbhost"
        assert profile.port == 5433
        assert profile.dbname == "mydb"
        assert profile.user == "admin"

    def test_from_dsn(self):
        profile = PgProfile(dsn="postgresql://myuser@dbhost:5433/mydb?sslmode=require")
        assert profile.host == "dbhost"
        assert profile.port == 5433
        assert profile.dbname == "mydb"
        assert profile.user == "myuser"
        assert profile.sslmode == "require"

    def test_dsn_with_field_override(self):
        profile = PgProfile(
            dsn="postgresql://user@staging-db.example.com/myapp",
            sslmode="verify-full",
            connect_timeout=5,
        )
        assert profile.host == "staging-db.example.com"
        assert profile.user == "user"
        assert profile.dbname == "myapp"
        assert profile.sslmode == "verify-full"
        assert profile.connect_timeout == 5

    def test_invalid_dsn_scheme(self):
        with pytest.raises(ConfigError, match="Invalid DSN scheme"):
            PgProfile(dsn="mysql://localhost/db")

    def test_connection_url_with_user_and_password(self):
        profile = PgProfile(
            host="dbhost", port=5432, dbname="mydb", user="admin", password="secret"
        )
        assert (
            profile.connection_url
            == "postgresql://admin:secret@dbhost:5432/mydb?sslmode=prefer"
        )

    def test_connection_url_with_user_only(self):
        profile = PgProfile(host="dbhost", dbname="mydb", user="admin")
        assert (
            profile.connection_url
            == "postgresql://admin@dbhost:5432/mydb?sslmode=prefer"
        )

    def test_connection_url_no_user(self):
        profile = PgProfile(host="dbhost", dbname="mydb")
        assert profile.connection_url == "postgresql://dbhost:5432/mydb?sslmode=prefer"

    def test_validate_sslmode_valid(self):
        for mode in (
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ):
            profile = PgProfile(sslmode=mode)
            assert profile.sslmode == mode

    def test_validate_sslmode_invalid(self):
        with pytest.raises(Exception, match="Invalid sslmode"):
            PgProfile(sslmode="invalid")

    def test_validate_port_valid(self):
        profile = PgProfile(port=1)
        assert profile.port == 1
        profile = PgProfile(port=65535)
        assert profile.port == 65535

    def test_validate_port_invalid_zero(self):
        with pytest.raises(Exception, match="Invalid port"):
            PgProfile(port=0)

    def test_validate_port_invalid_too_high(self):
        with pytest.raises(Exception, match="Invalid port"):
            PgProfile(port=65536)

    def test_model_fields_set_from_individual_fields(self):
        profile = PgProfile(host="dbhost", port=5433)
        assert "host" in profile.model_fields_set
        assert "port" in profile.model_fields_set
        assert "sslmode" not in profile.model_fields_set

    def test_model_fields_set_from_dsn(self):
        profile = PgProfile(dsn="postgresql://user@dbhost/mydb")
        assert "dsn" in profile.model_fields_set
        assert "host" in profile.model_fields_set
        assert "user" in profile.model_fields_set
        assert "dbname" in profile.model_fields_set


@pytest.mark.unit
class TestAppConfig:
    def test_default_values(self):
        config = AppConfig()
        assert config.default_timeout == 30.0
        assert config.default_format == "table"
        assert config.profiles == {}

    def test_with_profiles(self):
        config = AppConfig(
            profiles={
                "local": PgProfile(host="localhost", user="postgres"),
                "prod": PgProfile(dsn="postgresql://ro@prod/myapp"),
            }
        )
        assert "local" in config.profiles
        assert "prod" in config.profiles
        assert config.profiles["local"].host == "localhost"
        assert config.profiles["prod"].host == "prod"

    def test_custom_defaults(self):
        config = AppConfig(default_timeout=60.0, default_format="json")
        assert config.default_timeout == 60.0
        assert config.default_format == "json"


# ---------------------------------------------------------------------------
# Step 2.2: Configuration File Loading
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLoadConfig:
    def test_load_from_file(self, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(
            "default_timeout = 60\n"
            'default_format = "json"\n'
            "\n"
            "[profiles.local]\n"
            'host = "localhost"\n'
            "port = 5432\n"
            'dbname = "testdb"\n'
            'user = "testuser"\n'
        )
        config = load_config(config_file)
        assert config.default_timeout == 60.0
        assert config.default_format == "json"
        assert "local" in config.profiles
        assert config.profiles["local"].host == "localhost"
        assert config.profiles["local"].user == "testuser"

    def test_default_when_no_file(self, temp_dir):
        config = load_config(temp_dir / "nonexistent.toml")
        assert config.default_timeout == 30.0
        assert config.default_format == "table"
        assert config.profiles == {}

    def test_malformed_toml(self, temp_dir):
        config_file = temp_dir / "bad.toml"
        config_file.write_text("this is not valid = [toml {")
        with pytest.raises(ConfigError, match="Malformed TOML"):
            load_config(config_file)

    def test_invalid_config_values(self, temp_dir):
        config_file = temp_dir / "invalid.toml"
        config_file.write_text('[profiles.bad]\nsslmode = "invalid_mode"\n')
        with pytest.raises(ConfigError, match="Invalid configuration"):
            load_config(config_file)

    def test_load_with_dsn_profile(self, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(
            "[profiles.staging]\n"
            'dsn = "postgresql://deployer@staging-db.example.com/myapp"\n'
            'sslmode = "verify-full"\n'
            "connect_timeout = 5\n"
        )
        config = load_config(config_file)
        profile = config.profiles["staging"]
        assert profile.host == "staging-db.example.com"
        assert profile.user == "deployer"
        assert profile.dbname == "myapp"
        assert profile.sslmode == "verify-full"
        assert profile.connect_timeout == 5

    def test_empty_config_file(self, temp_dir):
        config_file = temp_dir / "empty.toml"
        config_file.write_text("")
        config = load_config(config_file)
        assert config.default_timeout == 30.0
        assert config.profiles == {}

    def test_config_with_unknown_keys(self, temp_dir):
        config_file = temp_dir / "extra.toml"
        config_file.write_text('unknown_key = "ignored"\nanother_unknown = 42\n')
        config = load_config(config_file)
        assert config.default_timeout == 30.0


# ---------------------------------------------------------------------------
# Step 2.3: Environment Variable Support
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEnvironmentVariables:
    def test_pghost_override(self, monkeypatch):
        monkeypatch.setenv("PGHOST", "envhost")
        config = AppConfig()
        resolved = resolve_config(config)
        assert resolved.host == "envhost"
        assert resolved.sources["host"] == "env: PGHOST"

    def test_pgport_override(self, monkeypatch):
        monkeypatch.setenv("PGPORT", "5433")
        config = AppConfig()
        resolved = resolve_config(config)
        assert resolved.port == 5433
        assert resolved.sources["port"] == "env: PGPORT"

    def test_pgdatabase_override(self, monkeypatch):
        monkeypatch.setenv("PGDATABASE", "envdb")
        config = AppConfig()
        resolved = resolve_config(config)
        assert resolved.dbname == "envdb"
        assert resolved.sources["dbname"] == "env: PGDATABASE"

    def test_pguser_override(self, monkeypatch):
        monkeypatch.setenv("PGUSER", "envuser")
        config = AppConfig()
        resolved = resolve_config(config)
        assert resolved.user == "envuser"
        assert resolved.sources["user"] == "env: PGUSER"

    def test_pgpassword_override(self, monkeypatch):
        monkeypatch.setenv("PGPASSWORD", "envpass")
        config = AppConfig()
        resolved = resolve_config(config)
        assert resolved.password == "envpass"
        assert resolved.sources["password"] == "env: PGPASSWORD"

    def test_env_does_not_override_cli(self, monkeypatch):
        monkeypatch.setenv("PGHOST", "envhost")
        config = AppConfig()
        resolved = resolve_config(config, host="clihost")
        assert resolved.host == "clihost"
        assert resolved.sources["host"] == "cli: --host"

    def test_missing_env_fallthrough(self):
        config = AppConfig()
        resolved = resolve_config(config)
        assert resolved.host == "localhost"
        assert resolved.sources["host"] == "default"

    def test_invalid_pgport_value(self, monkeypatch):
        monkeypatch.setenv("PGPORT", "not_a_number")
        config = AppConfig()
        with pytest.raises(ConfigError, match="Invalid PGPORT"):
            resolve_config(config)

    def test_env_overrides_profile(self, monkeypatch):
        monkeypatch.setenv("PGHOST", "envhost")
        config = AppConfig(
            profiles={"local": PgProfile(host="profilehost", user="profileuser")}
        )
        resolved = resolve_config(config, profile_name="local")
        assert resolved.host == "envhost"
        assert resolved.sources["host"] == "env: PGHOST"
        assert resolved.user == "profileuser"
        assert resolved.sources["user"] == "profile: local"


# ---------------------------------------------------------------------------
# Step 2.4: Configuration Precedence Resolver
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestResolveConfig:
    def test_defaults_only(self):
        config = AppConfig()
        resolved = resolve_config(config)
        assert resolved.host == "localhost"
        assert resolved.port == 5432
        assert resolved.dbname == "postgres"
        assert resolved.user is None
        assert resolved.sslmode == "prefer"
        assert resolved.default_timeout == 30.0
        assert resolved.default_format == "table"

    def test_cli_overrides_all(self, monkeypatch):
        monkeypatch.setenv("PGHOST", "envhost")
        config = AppConfig(
            profiles={"prod": PgProfile(host="prodhost", user="produser")}
        )
        resolved = resolve_config(
            config,
            profile_name="prod",
            host="clihost",
            user="cliuser",
        )
        assert resolved.host == "clihost"
        assert resolved.user == "cliuser"
        assert resolved.sources["host"] == "cli: --host"
        assert resolved.sources["user"] == "cli: --user"

    def test_dsn_overrides_env(self, monkeypatch):
        monkeypatch.setenv("PGHOST", "envhost")
        config = AppConfig()
        resolved = resolve_config(config, dsn="postgresql://dsnuser@dsnhost/dsndb")
        assert resolved.host == "dsnhost"
        assert resolved.user == "dsnuser"
        assert resolved.dbname == "dsndb"
        assert resolved.sources["host"] == "dsn"

    def test_env_overrides_profile(self, monkeypatch):
        monkeypatch.setenv("PGHOST", "envhost")
        config = AppConfig(profiles={"local": PgProfile(host="profilehost")})
        resolved = resolve_config(config, profile_name="local")
        assert resolved.host == "envhost"
        assert resolved.sources["host"] == "env: PGHOST"

    def test_profile_overrides_defaults(self):
        config = AppConfig(
            profiles={"prod": PgProfile(host="prodhost", port=5433, user="readonly")}
        )
        resolved = resolve_config(config, profile_name="prod")
        assert resolved.host == "prodhost"
        assert resolved.port == 5433
        assert resolved.user == "readonly"
        assert resolved.sources["host"] == "profile: prod"

    def test_full_precedence_chain(self, monkeypatch):
        monkeypatch.setenv("PGDATABASE", "envdb")
        config = AppConfig(
            default_timeout=60.0,
            profiles={
                "staging": PgProfile(
                    host="staging-host",
                    port=5433,
                    dbname="staging-db",
                    user="staging-user",
                )
            },
        )
        resolved = resolve_config(
            config,
            profile_name="staging",
            dsn="postgresql://dsnuser@dsnhost/dsndb",
            host="clihost",
        )
        assert resolved.host == "clihost"
        assert resolved.sources["host"] == "cli: --host"
        assert resolved.dbname == "dsndb"
        assert resolved.sources["dbname"] == "dsn"
        assert resolved.user == "dsnuser"
        assert resolved.sources["user"] == "dsn"
        assert resolved.port == 5433
        assert resolved.sources["port"] == "profile: staging"
        assert resolved.default_timeout == 60.0
        assert resolved.sources["default_timeout"] == "config"

    def test_config_file_defaults(self):
        config = AppConfig(default_timeout=60.0, default_format="json")
        resolved = resolve_config(config)
        assert resolved.default_timeout == 60.0
        assert resolved.default_format == "json"
        assert resolved.sources["default_timeout"] == "config"
        assert resolved.sources["default_format"] == "config"

    def test_resolved_config_model(self):
        resolved = ResolvedConfig(
            host="myhost",
            port=5433,
            sources={"host": "test"},
        )
        assert resolved.host == "myhost"
        assert resolved.port == 5433
        assert resolved.sources["host"] == "test"


# ---------------------------------------------------------------------------
# Step 2.5: Named Profiles
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNamedProfiles:
    def test_profile_selection(self):
        config = AppConfig(
            profiles={
                "local": PgProfile(host="localhost", user="postgres"),
                "prod": PgProfile(host="prod-db", user="readonly"),
            }
        )
        resolved = resolve_config(config, profile_name="prod")
        assert resolved.host == "prod-db"
        assert resolved.user == "readonly"
        assert resolved.active_profile == "prod"

    def test_unknown_profile_error(self):
        config = AppConfig(profiles={"local": PgProfile(host="localhost")})
        with pytest.raises(ConfigError, match="Unknown profile: 'nonexistent'"):
            resolve_config(config, profile_name="nonexistent")

    def test_unknown_profile_lists_available(self):
        config = AppConfig(
            profiles={
                "local": PgProfile(),
                "prod": PgProfile(),
            }
        )
        with pytest.raises(ConfigError, match="Available profiles: local, prod"):
            resolve_config(config, profile_name="missing")

    def test_unknown_profile_no_profiles(self):
        config = AppConfig()
        with pytest.raises(ConfigError, match="Available profiles: none"):
            resolve_config(config, profile_name="missing")

    def test_sql_profile_env(self, monkeypatch):
        monkeypatch.setenv("SQL_PROFILE", "staging")
        config = AppConfig(
            profiles={"staging": PgProfile(host="staging-host", user="deployer")}
        )
        resolved = resolve_config(config)
        assert resolved.host == "staging-host"
        assert resolved.user == "deployer"
        assert resolved.active_profile == "staging"

    def test_explicit_profile_overrides_env(self, monkeypatch):
        monkeypatch.setenv("SQL_PROFILE", "staging")
        config = AppConfig(
            profiles={
                "staging": PgProfile(host="staging-host"),
                "prod": PgProfile(host="prod-host"),
            }
        )
        resolved = resolve_config(config, profile_name="prod")
        assert resolved.host == "prod-host"
        assert resolved.active_profile == "prod"

    def test_profile_with_dsn(self):
        config = AppConfig(
            profiles={
                "remote": PgProfile(
                    dsn="postgresql://admin@remote-host:5433/appdb?sslmode=require"
                )
            }
        )
        resolved = resolve_config(config, profile_name="remote")
        assert resolved.host == "remote-host"
        assert resolved.port == 5433
        assert resolved.user == "admin"
        assert resolved.dbname == "appdb"
        assert resolved.sslmode == "require"

    def test_profile_only_applies_set_fields(self):
        config = AppConfig(
            profiles={"minimal": PgProfile(host="myhost", user="myuser")}
        )
        resolved = resolve_config(config, profile_name="minimal")
        assert resolved.host == "myhost"
        assert resolved.user == "myuser"
        assert resolved.port == 5432
        assert resolved.sources["port"] == "default"
        assert resolved.sslmode == "prefer"
        assert resolved.sources["sslmode"] == "default"


# ---------------------------------------------------------------------------
# Step 2.6: config show Command
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigShowCommand:
    def test_show_defaults(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text("")
        result = runner.invoke(app, ["--config", str(config_file), "config", "show"])
        assert result.exit_code == 0
        assert "host: localhost (default)" in result.stdout
        assert "port: 5432 (default)" in result.stdout
        assert "database: postgres (default)" in result.stdout
        assert "Active Profile: none" in result.stdout

    def test_show_with_profile(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text('[profiles.local]\nhost = "myhost"\nuser = "myuser"\n')
        result = runner.invoke(
            app,
            ["--config", str(config_file), "--profile", "local", "config", "show"],
        )
        assert result.exit_code == 0
        assert "host: myhost (profile: local)" in result.stdout
        assert "user: myuser (profile: local)" in result.stdout
        assert "Active Profile: local" in result.stdout

    def test_password_masked(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(
            "[profiles.secure]\n"
            'host = "dbhost"\n'
            'user = "admin"\n'
            'password = "supersecret"\n'
        )
        result = runner.invoke(
            app,
            ["--config", str(config_file), "--profile", "secure", "config", "show"],
        )
        assert result.exit_code == 0
        assert "supersecret" not in result.stdout
        assert "password: ***" in result.stdout

    def test_source_attribution_env(self, runner, temp_dir, monkeypatch):
        monkeypatch.setenv("PGHOST", "envhost")
        config_file = temp_dir / "config.toml"
        config_file.write_text("")
        result = runner.invoke(app, ["--config", str(config_file), "config", "show"])
        assert result.exit_code == 0
        assert "host: envhost (env: PGHOST)" in result.stdout

    def test_source_attribution_cli(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text("")
        result = runner.invoke(
            app,
            ["--config", str(config_file), "--host", "clihost", "config", "show"],
        )
        assert result.exit_code == 0
        assert "host: clihost (cli: --host)" in result.stdout

    def test_show_timeout_and_format(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text('default_timeout = 60\ndefault_format = "json"\n')
        result = runner.invoke(app, ["--config", str(config_file), "config", "show"])
        assert result.exit_code == 0
        assert "timeout: 60.0s (config)" in result.stdout
        assert "format: json (config)" in result.stdout


# ---------------------------------------------------------------------------
# Step 2.7: config profiles Command
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigProfilesCommand:
    def test_list_profiles(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(
            "[profiles.local]\n"
            'host = "localhost"\n'
            'user = "postgres"\n'
            "\n"
            "[profiles.production]\n"
            'host = "prod-db"\n'
            'user = "readonly"\n'
            'sslmode = "verify-full"\n'
        )
        result = runner.invoke(
            app, ["--config", str(config_file), "config", "profiles"]
        )
        assert result.exit_code == 0
        assert "local" in result.stdout
        assert "production" in result.stdout
        assert "prod-db" in result.stdout
        assert "verify-full" in result.stdout

    def test_active_profile_marked(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(
            "[profiles.local]\n"
            'host = "localhost"\n'
            "\n"
            "[profiles.prod]\n"
            'host = "prod-db"\n'
        )
        result = runner.invoke(
            app,
            [
                "--config",
                str(config_file),
                "--profile",
                "local",
                "config",
                "profiles",
            ],
        )
        assert result.exit_code == 0
        assert "* local (active)" in result.stdout
        assert "  prod" in result.stdout

    def test_no_profiles(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text("")
        result = runner.invoke(
            app, ["--config", str(config_file), "config", "profiles"]
        )
        assert result.exit_code == 0
        assert "No profiles configured" in result.stdout

    def test_profiles_show_connection_details(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(
            "[profiles.mydb]\n"
            'host = "myhost"\n'
            "port = 5433\n"
            'dbname = "mydb"\n'
            'user = "admin"\n'
        )
        result = runner.invoke(
            app, ["--config", str(config_file), "config", "profiles"]
        )
        assert result.exit_code == 0
        assert "host: myhost" in result.stdout
        assert "port: 5433" in result.stdout
        assert "database: mydb" in result.stdout
        assert "user: admin" in result.stdout


# ---------------------------------------------------------------------------
# Step 2.9: E2E Tests and Edge Cases
# ---------------------------------------------------------------------------

MULTI_PROFILE_CONFIG = (
    "default_timeout = 45\n"
    'default_format = "json"\n'
    "\n"
    "[profiles.local]\n"
    'host = "localhost"\n'
    "port = 5432\n"
    'dbname = "devdb"\n'
    'user = "devuser"\n'
    'password = "devpass"\n'
    "\n"
    "[profiles.staging]\n"
    'dsn = "postgresql://deployer@staging-db.example.com/myapp"\n'
    'sslmode = "verify-full"\n'
    "connect_timeout = 5\n"
    "\n"
    "[profiles.prod]\n"
    'dsn = "postgresql://readonly:s3cret@prod-db.example.com:5433/prodapp?sslmode=verify-full"\n'
)


@pytest.mark.e2e
class TestE2EConfigWorkflow:
    """Full CLI workflow from config file to resolved output."""

    def test_full_workflow_local_profile(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(MULTI_PROFILE_CONFIG)
        result = runner.invoke(
            app,
            ["--config", str(config_file), "--profile", "local", "config", "show"],
        )
        assert result.exit_code == 0
        assert "host: localhost (profile: local)" in result.stdout
        assert "port: 5432 (profile: local)" in result.stdout
        assert "database: devdb (profile: local)" in result.stdout
        assert "user: devuser (profile: local)" in result.stdout
        assert "password: *** (profile: local)" in result.stdout
        assert "timeout: 45.0s (config)" in result.stdout
        assert "format: json (config)" in result.stdout
        assert "Active Profile: local" in result.stdout

    def test_full_workflow_dsn_profile(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(MULTI_PROFILE_CONFIG)
        result = runner.invoke(
            app,
            ["--config", str(config_file), "--profile", "staging", "config", "show"],
        )
        assert result.exit_code == 0
        assert "host: staging-db.example.com (profile: staging)" in result.stdout
        assert "user: deployer (profile: staging)" in result.stdout
        assert "database: myapp (profile: staging)" in result.stdout
        assert "sslmode: verify-full (profile: staging)" in result.stdout

    def test_full_workflow_dsn_with_password(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(MULTI_PROFILE_CONFIG)
        result = runner.invoke(
            app,
            ["--config", str(config_file), "--profile", "prod", "config", "show"],
        )
        assert result.exit_code == 0
        assert "host: prod-db.example.com (profile: prod)" in result.stdout
        assert "port: 5433 (profile: prod)" in result.stdout
        assert "password: *** (profile: prod)" in result.stdout
        assert "s3cret" not in result.stdout

    def test_full_workflow_profiles_listing(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(MULTI_PROFILE_CONFIG)
        result = runner.invoke(
            app,
            [
                "--config",
                str(config_file),
                "--profile",
                "staging",
                "config",
                "profiles",
            ],
        )
        assert result.exit_code == 0
        assert "local" in result.stdout
        assert "* staging (active)" in result.stdout
        assert "prod" in result.stdout

    def test_full_precedence_cli_over_env_over_profile(
        self, runner, temp_dir, monkeypatch
    ):
        monkeypatch.setenv("PGDATABASE", "env-db")
        monkeypatch.setenv("PGUSER", "env-user")
        config_file = temp_dir / "config.toml"
        config_file.write_text(MULTI_PROFILE_CONFIG)
        result = runner.invoke(
            app,
            [
                "--config",
                str(config_file),
                "--profile",
                "local",
                "--host",
                "cli-host",
                "config",
                "show",
            ],
        )
        assert result.exit_code == 0
        assert "host: cli-host (cli: --host)" in result.stdout
        assert "database: env-db (env: PGDATABASE)" in result.stdout
        assert "user: env-user (env: PGUSER)" in result.stdout
        assert "port: 5432 (profile: local)" in result.stdout

    def test_dsn_flag_overrides_env_and_profile(self, runner, temp_dir, monkeypatch):
        monkeypatch.setenv("PGHOST", "env-host")
        config_file = temp_dir / "config.toml"
        config_file.write_text(MULTI_PROFILE_CONFIG)
        result = runner.invoke(
            app,
            [
                "--config",
                str(config_file),
                "--profile",
                "local",
                "--dsn",
                "postgresql://dsnuser@dsnhost:9999/dsndb",
                "config",
                "show",
            ],
        )
        assert result.exit_code == 0
        assert "host: dsnhost (dsn)" in result.stdout
        assert "port: 9999 (dsn)" in result.stdout
        assert "user: dsnuser (dsn)" in result.stdout
        assert "database: dsndb (dsn)" in result.stdout


@pytest.mark.e2e
class TestE2EEdgeCases:
    """Edge cases exercised through the full CLI path."""

    def test_malformed_toml_produces_helpful_error(self, runner, temp_dir):
        config_file = temp_dir / "bad.toml"
        config_file.write_text("[profiles.broken\nhost = ???")
        result = runner.invoke(app, ["--config", str(config_file), "config", "show"])
        assert result.exit_code != 0

    def test_missing_profile_lists_available(self, runner, temp_dir):
        config_file = temp_dir / "config.toml"
        config_file.write_text(MULTI_PROFILE_CONFIG)
        result = runner.invoke(
            app,
            [
                "--config",
                str(config_file),
                "--profile",
                "nonexistent",
                "config",
                "show",
            ],
        )
        assert result.exit_code != 0
        error_msg = str(result.exception)
        assert "Unknown profile: 'nonexistent'" in error_msg
        assert "local" in error_msg
        assert "staging" in error_msg
        assert "prod" in error_msg

    def test_empty_config_uses_defaults(self, runner, temp_dir):
        config_file = temp_dir / "empty.toml"
        config_file.write_text("")
        result = runner.invoke(app, ["--config", str(config_file), "config", "show"])
        assert result.exit_code == 0
        assert "host: localhost (default)" in result.stdout
        assert "port: 5432 (default)" in result.stdout
        assert "database: postgres (default)" in result.stdout
        assert "timeout: 30.0s (default)" in result.stdout
        assert "format: table (default)" in result.stdout
        assert "Active Profile: none" in result.stdout

    def test_unknown_keys_ignored(self, runner, temp_dir):
        config_file = temp_dir / "extra.toml"
        config_file.write_text(
            'unknown_key = "ignored"\n'
            "future_feature = true\n"
            "\n"
            "[profiles.local]\n"
            'host = "myhost"\n'
        )
        result = runner.invoke(
            app,
            ["--config", str(config_file), "--profile", "local", "config", "show"],
        )
        assert result.exit_code == 0
        assert "host: myhost (profile: local)" in result.stdout

    def test_nonexistent_config_file_uses_defaults(self, runner, temp_dir):
        result = runner.invoke(
            app,
            ["--config", str(temp_dir / "does_not_exist.toml"), "config", "show"],
        )
        assert result.exit_code == 0
        assert "host: localhost (default)" in result.stdout

    def test_password_from_env_masked_in_show(self, runner, temp_dir, monkeypatch):
        monkeypatch.setenv("PGPASSWORD", "super-secret-password")
        config_file = temp_dir / "config.toml"
        config_file.write_text("")
        result = runner.invoke(app, ["--config", str(config_file), "config", "show"])
        assert result.exit_code == 0
        assert "super-secret-password" not in result.stdout
        assert "password: *** (env: PGPASSWORD)" in result.stdout

    def test_sql_profile_env_selects_profile(self, runner, temp_dir, monkeypatch):
        monkeypatch.setenv("SQL_PROFILE", "prod")
        config_file = temp_dir / "config.toml"
        config_file.write_text(MULTI_PROFILE_CONFIG)
        result = runner.invoke(app, ["--config", str(config_file), "config", "show"])
        assert result.exit_code == 0
        assert "Active Profile: prod" in result.stdout
        assert "host: prod-db.example.com (profile: prod)" in result.stdout

    def test_cli_profile_overrides_sql_profile_env(self, runner, temp_dir, monkeypatch):
        monkeypatch.setenv("SQL_PROFILE", "prod")
        config_file = temp_dir / "config.toml"
        config_file.write_text(MULTI_PROFILE_CONFIG)
        result = runner.invoke(
            app,
            ["--config", str(config_file), "--profile", "local", "config", "show"],
        )
        assert result.exit_code == 0
        assert "Active Profile: local" in result.stdout
        assert "host: localhost (profile: local)" in result.stdout

    def test_all_env_vars_combined(self, runner, temp_dir, monkeypatch):
        monkeypatch.setenv("PGHOST", "envhost")
        monkeypatch.setenv("PGPORT", "9999")
        monkeypatch.setenv("PGDATABASE", "envdb")
        monkeypatch.setenv("PGUSER", "envuser")
        monkeypatch.setenv("PGPASSWORD", "envpass")
        config_file = temp_dir / "config.toml"
        config_file.write_text("")
        result = runner.invoke(app, ["--config", str(config_file), "config", "show"])
        assert result.exit_code == 0
        assert "host: envhost (env: PGHOST)" in result.stdout
        assert "port: 9999 (env: PGPORT)" in result.stdout
        assert "database: envdb (env: PGDATABASE)" in result.stdout
        assert "user: envuser (env: PGUSER)" in result.stdout
        assert "password: *** (env: PGPASSWORD)" in result.stdout

    def test_invalid_dsn_in_profile_produces_error(self, runner, temp_dir):
        config_file = temp_dir / "bad_dsn.toml"
        config_file.write_text('[profiles.bad]\ndsn = "mysql://not-postgres/db"\n')
        result = runner.invoke(
            app,
            ["--config", str(config_file), "--profile", "bad", "config", "show"],
        )
        assert result.exit_code != 0
