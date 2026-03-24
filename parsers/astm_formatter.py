"""
ASTM Message Formatter
Formats parsed ASTM data into human-readable clinical reports.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime


class ASTMFormatter:
    """Formats parsed ASTM messages into structured HTML reports."""

    def __init__(self):
        pass

    def format_as_html(self, parsed_data: Dict[str, Any]) -> str:
        """
        Generate an HTML fragment representing a clinical laboratory report.
       
        Args:
            parsed_data: Parsed ASTM message dictionary from ASTMParser

        Returns:
            HTML string (fragment, not full page)
        """
        sections = []

        # Header section
        if parsed_data.get('header'):
            sections.append(self._format_header(parsed_data['header']))

        # Patient demographics
        if parsed_data.get('patient'):
            sections.append(self._format_patient(parsed_data['patient']))

        # Order information
        if parsed_data.get('orders'):
            for order in parsed_data['orders']:
                sections.append(self._format_order(order))

        # Results table
        if parsed_data.get('results'):
            sections.append(self._format_results(parsed_data['results']))

        # Comments/Flags
        if parsed_data.get('comments'):
            sections.append(self._format_comments(parsed_data['comments']))

        # Manufacturer/QC info
        if parsed_data.get('manufacturer_records'):
            sections.append(self._format_manufacturer(parsed_data['manufacturer_records']))

        return '\n'.join(sections)

    def _format_header(self, header: Dict[str, Any]) -> str:
        """Format header section."""
        sender = header.get('sender_name', 'Unknown')
        version = header.get('version', 'LIS2-A2')
        timestamp = self._format_datetime(header.get('timestamp'))
        
        return f"""
<div class="report-section header-section">
    <div class="section-title">
        <i class="fas fa-file-medical"></i> Laboratory Report
    </div>
    <div class="header-info">
        <div class="info-row">
            <span class="label">Instrument:</span>
            <span class="value">{self._escape_html(sender)}</span>
        </div>
        <div class="info-row">
            <span class="label">Protocol:</span>
            <span class="value">{self._escape_html(version)}</span>
        </div>
        <div class="info-row">
            <span class="label">Message Time:</span>
            <span class="value">{timestamp}</span>
        </div>
    </div>
</div>"""

    def _format_patient(self, patient: Dict[str, Any]) -> str:
        """Format patient demographics section."""
        patient_name = self._format_name(patient.get('name'))
        patient_id = patient.get('practice_patient_id') or patient.get('lab_patient_id', 'N/A')
        dob = patient.get('date_of_birth', 'N/A')
        sex = patient.get('sex', 'U')
        
        return f"""
<div class="report-section patient-section">
    <div class="section-title">
        <i class="fas fa-user"></i> Patient Information
    </div>
    <div class="patient-info">
        <div class="info-row">
            <span class="label">Name:</span>
            <span class="value name">{self._escape_html(patient_name)}</span>
        </div>
        <div class="info-row">
            <span class="label">Patient ID:</span>
            <span class="value">{self._escape_html(patient_id)}</span>
        </div>
        <div class="info-row">
            <span class="label">Date of Birth:</span>
            <span class="value">{self._escape_html(dob)}</span>
        </div>
        <div class="info-row">
            <span class="label">Sex:</span>
            <span class="value">{self._escape_html(sex)}</span>
        </div>
    </div>
</div>"""

    def _format_order(self, order: Dict[str, Any]) -> str:
        """Format order information section."""
        test_name = 'Unknown Test'
        specimen_type = 'N/A'
        
        test_id = order.get('universal_test_id')
        if test_id:
            test_name = test_id.get('display_name') or test_id.get('test_name') or test_id.get('test_id', 'Unknown')
            specimen_type = test_id.get('specimen_type', 'N/A')
        
        specimen = order.get('specimen_descriptor', specimen_type)
        collection_time = self._format_datetime(order.get('collection_datetime'))
        report_type = self._format_report_type(order.get('report_type'))
        
        return f"""
<div class="report-section order-section">
    <div class="section-title">
        <i class="fas fa-flask"></i> Test Order
    </div>
    <div class="order-info">
        <div class="info-row">
            <span class="label">Test:</span>
            <span class="value test-name">{self._escape_html(test_name)}</span>
        </div>
        <div class="info-row">
            <span class="label">Specimen:</span>
            <span class="value">{self._escape_html(specimen)}</span>
        </div>
        <div class="info-row">
            <span class="label">Collection Time:</span>
            <span class="value">{collection_time}</span>
        </div>
        <div class="info-row">
            <span class="label">Report Status:</span>
            <span class="value status-{report_type.lower()}">{report_type}</span>
        </div>
    </div>
</div>"""

    def _format_results(self, results: List[Dict[str, Any]]) -> str:
        """Format results as a table."""
        if not results:
            return ""
        
        rows = []
        for result in results:
            test_id_dict = result.get('universal_test_id', {})
            test_name = test_id_dict.get('display_name') or test_id_dict.get('test_name') or test_id_dict.get('test_id', 'Unknown')
            
            value = result.get('value', '--')
            units = result.get('units', '')
            ref_range = result.get('reference_range', '')
            flags = result.get('abnormal_flags', '')
            status = result.get('result_status', 'F')
            
            # Determine row class based on flags
            row_class = ''
            flag_icon = ''
            if flags:
                flags_upper = flags.upper()
                if 'H' in flags_upper or 'HH' in flags_upper:
                    row_class = 'abnormal-high'
                    flag_icon = '<i class="fas fa-arrow-up text-danger"></i>'
                elif 'L' in flags_upper or 'LL' in flags_upper:
                    row_class = 'abnormal-low'
                    flag_icon = '<i class="fas fa-arrow-down text-danger"></i>'
                elif 'A' in flags_upper:
                    row_class = 'abnormal'
                    flag_icon = '<i class="fas fa-exclamation-triangle text-warning"></i>'
            
            rows.append(f"""
                <tr class="{row_class}">
                    <td class="test-name">{self._escape_html(test_name)}</td>
                    <td class="value">{self._escape_html(value)}</td>
                    <td class="units">{self._escape_html(units)}</td>
                    <td class="ref-range">{self._escape_html(ref_range)}</td>
                    <td class="flags">{flag_icon} {self._escape_html(flags)}</td>
                    <td class="status">{self._escape_html(status)}</td>
                </tr>""")
        
        return f"""
<div class="report-section results-section">
    <div class="section-title">
        <i class="fas fa-chart-line"></i> Test Results
    </div>
    <div class="results-table-container">
        <table class="results-table">
            <thead>
                <tr>
                    <th>Test</th>
                    <th>Value</th>
                    <th>Units</th>
                    <th>Reference Range</th>
                    <th>Flags</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
    </div>
</div>"""

    def _format_comments(self, comments: List[Dict[str, Any]]) -> str:
        """Format comments and instrument flags section."""
        if not comments:
            return ""
        
        comment_items = []
        for comment in comments:
            source = comment.get('comment_source', 'I')
            source_label = {'I': 'Instrument', 'L': 'Lab', 'P': 'Provider'}.get(source, source)
            
            # Check for parsed comments (individual flags)
            parsed = comment.get('parsed_comments')
            if parsed and len(parsed) > 1:
                # Show individual flags as badges
                flags = [f'<span class="flag-badge">{self._escape_html(str(f))}</span>' 
                        for f in parsed if f]
                comment_items.append(
                    f'<div class="comment-item"><span class="comment-source">[{source_label}]</span> {" ".join(flags)}</div>'
                )
            else:
                # Show full comment text
                text = comment.get('comment_text', '')
                if text:
                    comment_items.append(
                        f'<div class="comment-item"><span class="comment-source">[{source_label}]</span> {self._escape_html(text)}</div>'
                    )
        
        if not comment_items:
            return ""
        
        return f"""
<div class="report-section comments-section">
    <div class="section-title">
        <i class="fas fa-comment-medical"></i> Comments & Flags
    </div>
    <div class="comments-list">
        {''.join(comment_items)}
    </div>
</div>"""

    def _format_manufacturer(self, mfr_records: List[Dict[str, Any]]) -> str:
        """Format manufacturer/reagent information."""
        if not mfr_records:
            return ""
        
        items = []
        for record in mfr_records:
            scope = record.get('definition_scope', '')
            name = record.get('definition_name', '')
            fields = record.get('implementation_fields', [])
            
            if name:
                # For reagent info, parse lot numbers and expiry dates
                if scope == 'REAGENT' and fields:
                    reagent_info = ' | '.join([str(f) for f in fields if f])
                    items.append(f'<div class="mfr-item"><strong>{self._escape_html(name)}:</strong> {self._escape_html(reagent_info)}</div>')
                else:
                    items.append(f'<div class="mfr-item"><strong>{self._escape_html(name)}</strong></div>')
        
        if not items:
            return ""
        
        return f"""
<div class="report-section mfr-section">
    <div class="section-title">
        <i class="fas fa-cogs"></i> Quality Control & Reagents
    </div>
    <div class="mfr-info">
        {''.join(items)}
    </div>
</div>"""

    # Helper methods
    def _format_name(self, name_dict: Optional[Dict[str, Any]]) -> str:
        """Format a name dictionary into a string."""
        if not name_dict:
            return "Unknown"
        
        parts = []
        if name_dict.get('last'):
            parts.append(name_dict['last'])
        if name_dict.get('first'):
            parts.append(name_dict['first'])
        if name_dict.get('middle'):
            parts.append(name_dict['middle'])
        
        return ', '.join(parts) if parts else "Unknown"

    def _format_datetime(self, dt_str: Optional[str]) -> str:
        """Format datetime string for display."""
        if not dt_str:
            return "N/A"
        
        # Try to parse common ASTM date formats: YYYYMMDDHHmmss
        try:
            if len(dt_str) == 14:  # YYYYMMDDHHmmss
                dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            elif len(dt_str) == 8:  # YYYYMMDD
                dt = datetime.strptime(dt_str, '%Y%m%d')
                return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass
        
        return dt_str

    def _format_report_type(self, report_type: Optional[str]) -> str:
        """Format report type code into readable string."""
        if not report_type:
            return "Unknown"
        
        type_map = {
            'F': 'Final',
            'P': 'Preliminary',
            'C': 'Corrected',
            'X': 'Not Performed',
            'O': 'Order',
            'I': 'Incomplete'
        }
        
        return type_map.get(report_type.upper(), report_type)

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        if text is None:
            return ""
        
        text = str(text)
        return (text
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#39;'))


__all__ = ['ASTMFormatter']
