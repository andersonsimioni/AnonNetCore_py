#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p build

if command -v latexmk >/dev/null 2>&1; then
    latexmk -pdf -interaction=nonstopmode -halt-on-error -outdir=build main.tex
else
    pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build main.tex

    if grep -q "\\\\citation" build/main.aux 2>/dev/null; then
        bibtex build/main
    fi

    pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build main.tex
    pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build main.tex
fi

echo "PDF generated at $SCRIPT_DIR/build/main.pdf"
