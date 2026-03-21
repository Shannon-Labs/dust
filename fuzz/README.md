# Fuzz

Early fuzz targets should focus on:

- SQL lexer and parser input handling
- page and WAL header decoding
- manifest parsing
- lockfile and migration metadata parsing

The initial bootstrap does not include fuzz targets yet, but the repo is organized so they can be added without restructuring.

