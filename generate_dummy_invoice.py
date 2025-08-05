from datetime import datetime
from io import BytesIO

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer


def generate_bicycle_insurance_invoice() -> BytesIO:
    # Create the PDF document

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=2 * cm, bottomMargin=2 * cm)

    # Get sample styles and create custom styles
    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=colors.darkblue,
        spaceAfter=20,
        alignment=TA_CENTER
    )

    subtitle_style = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.darkblue,
        spaceAfter=12,
        spaceBefore=15
    )

    normal_style = ParagraphStyle(
        'CustomNormal',
        parent=styles['Normal'],
        fontSize=10,
        spaceAfter=6
    )

    bold_style = ParagraphStyle(
        'CustomBold',
        parent=styles['Normal'],
        fontSize=10,
        spaceAfter=6,
        fontName='Helvetica-Bold'
    )

    # Story elements
    story = []

    # Header with company info and date
    header_data = [
        ["BICYCLE INSURANCE CLAIM", "", "Claim Date: " + datetime.now().strftime("%d.%m.%Y")],
        ["Partial Damage Claim Invoice", "", "Claim ID: PDC-2024-0622-AB"]
    ]

    header_table = Table(header_data, colWidths=[8 * cm, 4 * cm, 6 * cm])
    header_table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (0, 0), 16),
        ('FONTNAME', (0, 1), (0, 1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 1), (0, 1), 12),
        ('TEXTCOLOR', (0, 0), (0, 1), colors.darkblue),
        ('ALIGN', (2, 0), (2, -1), 'RIGHT'),
        ('FONTSIZE', (2, 0), (2, -1), 10),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))

    story.append(header_table)
    story.append(Spacer(1, 20))

    # Claim Type
    story.append(Paragraph("<b>CLAIM TYPE:</b> Partial Damage (Accidental Damage - Not Total Loss)", bold_style))
    story.append(Spacer(1, 15))

    # 1 - Policyholder Information
    story.append(Paragraph("Policyholder Information", subtitle_style))

    policyholder_data = [
        ["Name:", "Anna Becker"],
        ["Address:", "Uhlandstraße 20, 80336 München"],
        ["Insurance Policy Number:", "BIKE-3421987"]
    ]

    policyholder_table = Table(policyholder_data, colWidths=[5 * cm, 10 * cm])
    policyholder_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))

    story.append(policyholder_table)
    story.append(Spacer(1, 15))

    # 2 - Insurer Information
    story.append(Paragraph("Insurer / Repair Shop Information", subtitle_style))

    insurer_data = [
        ["Name:", "Fahrrad Wagner GmbH"],
        ["Address:", "Musterstraße 12, 10115 Berlin"],
        ["IBAN:", "DE40 7001 1111 3456 7890 00"],
    ]

    insurer_table = Table(insurer_data, colWidths=[5 * cm, 10 * cm])
    insurer_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))

    story.append(insurer_table)
    story.append(Spacer(1, 15))

    # 3 - Bicycle Details
    story.append(Paragraph("Bicycle Details", subtitle_style))

    bicycle_data = [
        ["Manufacturer & Model:", "Cube Nature Pro 2023"],
        ["Serial Number:", "CUBE9876543"],
        ["Date of Purchase:", "01.04.2023"]
    ]

    bicycle_table = Table(bicycle_data, colWidths=[5 * cm, 10 * cm])
    bicycle_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))

    story.append(bicycle_table)
    story.append(Spacer(1, 15))

    # 4 - Incident Description
    story.append(Paragraph("Incident Description", subtitle_style))

    incident_date_label = Paragraph("<b>Incident Date:</b>", normal_style)
    incident_date_value = Paragraph("22.06.2024", normal_style)

    damage_circumstances_label = Paragraph("<b>Damage Circumstances:</b>", normal_style)
    damage_circumstances_value = Paragraph(
        "While parked outside work, a car reversed into my bike, bending the rear wheel and breaking the rear derailleur. The frame and main mechanicals are otherwise undamaged.",
        normal_style)

    police_report_label = Paragraph("<b>Police Report:</b>", normal_style)
    police_report_value = Paragraph("Not required, as this was accidental damage and not vandalism or theft.",
                                    normal_style)

    incident_data = [
        [incident_date_label, incident_date_value],
        [damage_circumstances_label, damage_circumstances_value],
        [police_report_label, police_report_value]
    ]

    incident_table = Table(incident_data, colWidths=[5 * cm, 10 * cm])
    incident_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))

    story.append(incident_table)
    story.append(Spacer(1, 15))

    # 5 - Documentation Provided
    story.append(Paragraph("Documentation Provided", subtitle_style))

    additional_docs = [
        "• Photos of damage (rear wheel, broken derailleur)",
        "• Workshop assessment and repair estimate",
        "• Original purchase receipt (for proof of value)",
        "• Completed claim form"
    ]

    for additional_doc in additional_docs:
        story.append(Paragraph(additional_doc, normal_style))

    story.append(Spacer(1, 15))

    # 6 - Repair Invoice
    story.append(Paragraph("Repair Invoice", subtitle_style))

    # Invoice table header
    invoice_header = [["Position", "Description", "Quantity", "Unit Price", "Total"]]

    # Invoice data
    invoice_data = [
        ["1", "Rear wheel replacement", "1", "110.00€", "110.00€"],
        ["2", "Rear derailleur, Shimano", "1", "65.00€", "65.00€"],
        ["3", "Labor (wheel install)", "1", "25.00€", "25.00€"]
    ]

    # Combine header and data
    full_invoice_data = invoice_header + invoice_data

    invoice_table = Table(full_invoice_data, colWidths=[2 * cm, 6 * cm, 2 * cm, 3 * cm, 3 * cm])
    invoice_table.setStyle(TableStyle([
        # Header styling
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),

        # Data styling
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('ALIGN', (0, 1), (0, -1), 'CENTER'),  # Position column
        ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # Description column
        ('ALIGN', (2, 1), (-1, -1), 'CENTER'),  # Quantity, Unit Price, Total

        # Grid
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
    ]))

    story.append(invoice_table)
    story.append(Spacer(1, 10))

    # Totals table
    totals_data = [
        ["", "", "", "Subtotal", "200.00€"],
        ["", "", "", "VAT 19%", "38.00€"],
        ["", "", "", "Total (Gross)", "238.00€"]
    ]

    totals_table = Table(totals_data, colWidths=[2 * cm, 6 * cm, 2 * cm, 3 * cm, 3 * cm])
    totals_table.setStyle(TableStyle([
        ('FONTNAME', (3, 0), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (3, 0), (-1, -1), 10),
        ('ALIGN', (3, 0), (-1, -1), 'CENTER'),
        ('LINEBELOW', (3, 0), (-1, 0), 1, colors.black),
        ('LINEBELOW', (3, 1), (-1, 1), 1, colors.black),
        ('LINEBELOW', (3, 2), (-1, 2), 2, colors.black),
        ('BACKGROUND', (3, 2), (-1, 2), colors.lightgrey),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
    ]))

    story.append(totals_table)
    story.append(Spacer(1, 15))

    # 6 - Claim Statement
    story.append(Paragraph("Claim Statement / Settlement Request", subtitle_style))

    claim_statement = """I request reimbursement for the cost of repairing the damaged parts, as per attached invoice and photographic evidence. The damage does not meet criteria for total loss. Please process as <b>partial damage</b> under my policy's accidental damage clause."""

    story.append(Paragraph(claim_statement, normal_style))
    story.append(Spacer(1, 30))

    # Footer with signature section
    footer_data = [
        ["Policyholder Signature:", "_" * 30, "Date:", "_" * 15],
        ["", "", "", ""],
        ["Anna Becker", "", datetime.now().strftime("%d.%m.%Y"), ""]
    ]

    footer_table = Table(footer_data, colWidths=[4 * cm, 6 * cm, 2 * cm, 4 * cm])
    footer_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'BOTTOM'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))

    story.append(footer_table)

    # Build the PDF
    doc.build(story)

    buffer.seek(0)
    pdf_bytes = buffer.getvalue()
    buffer.close()

    print("PDF invoice created successfully in memory")

    with open("test.pdf", 'wb') as f:
        f.write(pdf_bytes)
    print(f"PDF saved to {len(pdf_bytes)}")

    return pdf_bytes


def generate_and_save_invoice_to_file(filename: str = "bicycle_insurance_claim_invoice.pdf"):
    """Helper function to save the PDF to a file if needed"""
    pdf_bytes = generate_bicycle_insurance_invoice()
    with open(filename, 'wb') as f:
        f.write(pdf_bytes)
    print(f"PDF saved to {filename}")

# # Create the invoice
# if __name__ == "__main__":
#     generate_and_save_invoice_to_file()