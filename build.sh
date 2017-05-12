#!/usr/bin/env bash
set -eu

dotnet restore
dotnet build

# Tests
dotnet test Common/Snappy.Sharp.Test/Snappy.Sharp.Test.csproj
dotnet test Common/Crc32C.Sharp.Test/Crc32C.Sharp.Test.csproj
dotnet test LevelDB-Test/LevelDB-Test.csproj
