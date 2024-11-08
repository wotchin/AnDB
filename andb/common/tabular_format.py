class TabularFormat:
    def __init__(self):
        self.field_names = []
        self.rows = []
        self._align = 'l'  # Default left alignment for data
        
    def add_row(self, row):
        """Add a row to the table."""
        self.rows.append(row)

    def _get_column_widths(self):
        """Calculate the maximum width needed for each column."""
        widths = [len(str(field)) for field in self.field_names]
        
        for row in self.rows:
            for i, value in enumerate(row):
                # Split value into lines if it contains newlines
                value_lines = str(value).split('\n')
                # Get max width from all lines
                max_line_width = max(len(line) for line in value_lines)
                widths[i] = max(widths[i], max_line_width)
                
        return widths

    def _create_separator(self, widths):
        """Create horizontal separator line."""
        parts = []
        for w in widths:
            parts.append('-' * (w + 2))  # +2 for padding spaces
        return '+' + '+'.join(parts) + '+'

    def _format_row(self, row, widths, align='l'):
        """Format a single row with given alignment."""
        # Convert all values to strings and split into lines
        value_lines = []
        max_lines = 1
        
        for value in row:
            lines = str(value).split('\n')
            value_lines.append(lines)
            max_lines = max(max_lines, len(lines))
            
        # Pad all values to have the same number of lines
        for lines in value_lines:
            while len(lines) < max_lines:
                lines.append('')
                
        # Format each line
        result = []
        for line_idx in range(max_lines):
            parts = []
            for col_idx, lines in enumerate(value_lines):
                line = lines[line_idx]
                width = widths[col_idx]
                if align == 'l':
                    parts.append(f' {line:<{width}} ')
                else:  # center alignment
                    parts.append(f' {line:^{width}} ')
            result.append('|' + '|'.join(parts) + '|')
            
        return '\n'.join(result)

    def get_string(self):
        """Generate the formatted table string."""
        if not self.field_names or not self.rows:
            return "(empty)"

        # Calculate column widths
        widths = self._get_column_widths()
        
        # Build table string
        lines = []
        
        # Add top border
        lines.append(self._create_separator(widths))
        
        # Add header with center alignment
        lines.append(self._format_row(self.field_names, widths, align='c'))
        
        # Add header-data separator
        lines.append(self._create_separator(widths))
        
        # Add data rows with left alignment
        for row in self.rows:
            lines.append(self._format_row(row, widths, align='l'))
            
        # Add bottom border
        lines.append(self._create_separator(widths))
        
        return '\n'.join(lines)
