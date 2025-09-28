{ pkgs ? import <nixpkgs> {} }:

pkgs.stdenv.mkDerivation rec {
  pname = "dvdbackup";
  version = "0.4.2-gaps";

  src = ./.;

  nativeBuildInputs = with pkgs; [
    autoreconfHook
    gettext
    pkg-config
  ];

  buildInputs = with pkgs; [
    libdvdread
    libdvdcss
    dvdauthor
  ];

  meta = with pkgs.lib; {
    description = "Tool to rip video DVDs from the command line (with gaps resume support)";
    homepage = "https://dvdbackup.sourceforge.net/";
    license = licenses.gpl3Plus;
    maintainers = [ maintainers.bradediger ];
    platforms = platforms.linux ++ platforms.darwin;
    mainProgram = "dvdbackup";
  };
}
