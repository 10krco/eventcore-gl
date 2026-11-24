{
  description = "EventCore development environment for Gleam on Erlang/OTP";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/31ecbaca1bdefa45dffc409f0250bd8039ea0010";

  outputs = { nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
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
        packages = with pkgs; [ gleam beamPackages.erlang beamPackages.rebar3 lefthook git gh tiber util-linux ];
      };
    };
}
