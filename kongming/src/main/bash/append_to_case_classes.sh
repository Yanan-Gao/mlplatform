#!/bin/bash
# usage: sh append_to_case_classes.sh
#
# Reads an input file named "new_fields_to_append.txt" in the same folder:
#   - Skips commentary lines starting with '#'
#   - The first non-comment line is a space-separated list of case class names
#   - The subsequent non-comment lines are new fields to add
#
# For each specified case class, the script searches in
# ../scala/com/thetradedesk/kongming/datasets
# finds the parameter list, and appends the new fields at the end.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INPUT_FILE="$SCRIPT_DIR/new_fields_to_append.txt"

if [ ! -f "$INPUT_FILE" ]; then
    echo "ERROR: $INPUT_FILE does not exist."
    exit 1
fi

# Filter out comment lines (lines starting with '#')
non_comment_lines=$(grep -v '^#' "$INPUT_FILE")

# First non-comment line => case class names
classes_input=$(echo "$non_comment_lines" | head -n 1)
# Remaining non-comment lines => new fields
fields_str=$(echo "$non_comment_lines" | tail -n +2)

echo "Case classes to update: $classes_input"
echo "New fields to add:"
echo "$fields_str"
echo "----------------------------------------"

# Directory containing the Scala files to modify
DATASET_DIR="$SCRIPT_DIR/../scala/com/thetradedesk/kongming/datasets"

# Export new fields for Perl; escape backslashes and quotes
NEW_FIELDS=$(echo "$fields_str" | sed 's/\\/\\\\/g; s/"/\\"/g')
export NEW_FIELDS

for class in $classes_input; do
    echo "Processing case class: $class"
    # Find files that contain "case class <class>("
    files=$(grep -Rl "case class $class(" "$DATASET_DIR")
    if [ -z "$files" ]; then
        echo "  No files found for $class"
        continue
    fi

    export CLASS_NAME="$class"

    for file in $files; do
        echo "  Updating file: $file"
        perl -0777 -i -pe "$(cat <<'PERL_CODE'
use strict;
use warnings;

my $newFields = $ENV{"NEW_FIELDS"};
my $className = $ENV{"CLASS_NAME"};

# Capture everything from "case class <className>(" up to the first occurrence
# of a line that consists only of optional whitespace followed by ")"
s{
  (case\s+class\s+\Q$className\E\s*\(.*?)
  (\n\s*\))
}{
  do {
      my $header  = $1;  # Parameter block (excluding the final closing line)
      my $closing = $2;  # The closing parenthesis line (starts with newline + optional spaces + ')')

      # Split the header into lines; pop off the last parameter line
      my @lines = split /\n/, $header;
      my $last_line = pop @lines;
      if (!defined $last_line) {
          # Edge case: no last line found => just return as-is
          $header . $closing
      } else {
          # Trim trailing spaces
          $last_line =~ s/\s+$//;

          # Detect indentation from the start of the last line
          my ($indent) = $last_line =~ /^(\s+)/;
          $indent = "    " unless defined $indent;

          # Ensure the last parameter ends with a comma
          $last_line .= "," unless $last_line =~ /,\s*$/;

          # Put the last line back
          push @lines, $last_line;
          # Rebuild the header without adding an extra newline
          $header = join "\n", @lines;

          # Prepare new fields
          my @fields = split /\n/, $newFields;
          my $formatted_fields = "";
          for my $i (0 .. $#fields) {
              my $field = $fields[$i];
              $field =~ s/^\s+//;
              $field =~ s/\s+$//;
              # For each field except the last, add a comma and newline
              if ($i < $#fields) {
                  $formatted_fields .= "\n$indent$field,";
              } else {
                  # The last field => no trailing newline here
                  $formatted_fields .= "\n$indent$field";
              }
          }

          # Combine: existing params + formatted new fields + closing line
          $header . $formatted_fields . $closing;
      }
  }
}gesx;
PERL_CODE
)" "$file"
    done
done

echo "Update complete."