#!/usr/bin/env bash

# Copyright (c) 2022 Daniél Kerkmann <daniel@kerkmann.dev>
# and the contributors of the MarsValley project.
#
# Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
# the European Commission - subsequent versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the Licence.
# You may obtain a copy of the Licence at:
#
# https://joinup.ec.europa.eu/software/page/eupl
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the Licence is distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Licence for the specific language governing permissions and
# limitations under the Licence.

script_description="Check if the license header is set in all files."
script_dir=$(dirname "$0")
script_filename=$(basename "$0")

cd "$script_dir"/..

if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    echo "Usage: $script_filename [options]"
    echo "$script_description"
    echo ""
    echo "  -v, --verbose   be more verbose, i.e. print all checked files and hashes"
    echo "  -h, --help      show this help message and exit"
    exit 0
fi
if [ "$1" == "--verbose" ] || [ "$1" == "-v" ]; then
    verbose="true"
else
    verbose="false"
fi

[ "$verbose" == "true" ] && echo "$script_description"

license=$(cat<<EOF
/*
* The MIT License (MIT)
*
* Copyright (c) 2023 Daniél Kerkmann <daniel@kerkmann.dev>
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*/
EOF
)

# ignore lines starting with '* Copyright' in license header
md5sum_license=$(echo "$license" | grep -v '* Copyright' | md5sum | cut -f1 -d ' ')

[ "$verbose" == "true" ] && echo "License md5sum: $md5sum_license"

status=0
files_checked=0

while IFS= read -r -d '' file; do
    [ "$verbose" == "true" ] && echo "Check source file: $file"
    first_line=$(head -n 1 $file)
    if [ "$first_line" != "/*" ]; then
        echo -e "\e[31mVerification failed for file: $file!\e[0m"
        status=`expr $status + 1`
        files_checked=`expr $files_checked + 1`
        continue
    fi
    # get the line of the first end of comment block '*/' and read until that line
    num_header_lines=$(grep -En '(^\*/$)' $file | head -n 1 | cut -f1 -d ':')
    file_header=`head -n $num_header_lines $file`
    # ignore lines starting with '* Copyright' for checksum comparison
    md5sum_file=`echo "$file_header" | grep -v '* Copyright' | md5sum | cut -f1 -d ' '`
    [ "$verbose" == "true" ] && echo "File md5sum: $md5sum_file"
    if [ "$md5sum_license" != "$md5sum_file" ]; then
        echo -e "\e[31mVerification failed for file: $file!\e[0m"
        status=`expr $status + 1`
    fi

    files_checked=`expr $files_checked + 1`
done < <(find src -type f -name '*.rs' -print0)

if [ $status -gt 0 ]; then
    echo -e "\n\e[31m(╯°□°)╯︵ ┻━┻   $status of $files_checked files have a missing/wrong license header!\e[0m"
else
    echo -e "\e[32m(⊃｡•́‿•̀｡)⊃   Checked $files_checked files, all of them have a valid license header! Hurray!\e[0m"
fi
exit $status
