rm -rf build

cmake -S . -B build/dev -D CMAKE_BUILD_TYPE=Release
cmake --build build/dev -j$(nproc)
