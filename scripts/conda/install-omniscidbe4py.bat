cmake --install . --component "include" --prefix %CONDA_PREFIX%/include/omnisci
cmake --install . --component "doc" --prefix %CONDA_PREFIX%/share/doc/omnisci
cmake --install . --component "data" --prefix %CONDA_PREFIX%/opt/omnisci
cmake --install . --component "thrift" --prefix %CONDA_PREFIX%/opt/omnisci
cmake --install . --component "QE" --prefix %CONDA_PREFIX%
cmake --install . --component "jar" --prefix %CONDA_PREFIX%
cmake --install . --component "Unspecified" --prefix %CONDA_PREFIX%/opt/omnisci
cmake --install . --component "exe" --prefix %CONDA_PREFIX%
cmake --install . --component "DBE" --prefix %CONDA_PREFIX%/Library
python Embedded\setup.py build_ext -f install