{
  description = "EventCore development environment for Gleam on Erlang/OTP";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/31ecbaca1bdefa45dffc409f0250bd8039ea0010";

  outputs = { nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      # Resolve the driver's -lsqlite3 to the pinned SQLCipher library.
      sqlcipherSqlite = pkgs.runCommand "sqlcipher-sqlite-compat" { } ''
        mkdir -p "$out/include" "$out/lib"
        ln -s ${pkgs.lib.getDev pkgs.sqlcipher}/include/sqlcipher/sqlite3.h "$out/include/sqlite3.h"
        printf 'INPUT ( %s/lib/libsqlcipher.so )\n' ${pkgs.lib.getLib pkgs.sqlcipher} > "$out/lib/libsqlite3.so"
      '';
      tiber = pkgs.stdenv.mkDerivation rec {
        pname = "tiber";
        version = "1.5.1";
        src = pkgs.fetchurl {
          url = "https://github.com/jwilger/ai-plugins/releases/download/tiber-v${version}/tiber-v${version}-linux-x86_64.tar.gz";
          sha256 = "855af338fc388b6eddf8bb0d29445bf84545ea2aeadd9e55618a639b23d6f558";
        };
        nativeBuildInputs = [ pkgs.autoPatchelfHook ];
        buildInputs = [ pkgs.stdenv.cc.cc.lib pkgs.openssl ];
        dontBuild = true;
        installPhase = ''
          install -Dm755 tiber "$out/bin/tiber"
          install -Dm644 LICENSE "$out/share/licenses/tiber/LICENSE"
        '';
      };
    in {
      devShells.${system}.default = pkgs.mkShell {
        EVENTCORE_SQLITE_TEST_BACKEND = "sqlcipher";
        USE_SYSTEM_SQLITE = "1";
        CFLAGS = "-I${sqlcipherSqlite}/include";
        LDFLAGS = "-L${sqlcipherSqlite}/lib";
        packages = with pkgs; [ gleam beamPackages.erlang beamPackages.rebar3 lefthook git gh tiber util-linux ];
      };
    };
}
