# -*- coding: utf-8 -*-
"""
lori.io.pdf.template
~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from typing import Tuple

import numpy as np

from reportlab.lib import pagesizes
from reportlab.lib.colors import Color
from reportlab.lib.utils import ImageReader
from reportlab.lib.units import mm
from reportlab.pdfgen.canvas import Canvas
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import BaseDocTemplate, PageTemplate, Frame
from .styles import PdfStyles


class PdfDocTemplate(BaseDocTemplate):
    MARGIN_LEFT:   float = 25 * mm
    MARGIN_RIGHT:  float = 25 * mm
    MARGIN_TOP:    float = 30 * mm
    MARGIN_BOTTOM: float = 25 * mm

    def __init__(
        self,
         styles: PdfStyles,
         filename: str,
         creation_date: str,
         logo_cover: str = None,
         logo_header: str = None,
         confidential: bool = False,
         pagesize: Tuple[float, float] = pagesizes.portrait(pagesizes.A4),
         margin_left: float = MARGIN_LEFT,
         margin_right: float = MARGIN_RIGHT,
         margin_top: float = MARGIN_TOP,
         margin_bottom: float = MARGIN_BOTTOM,
         **kwargs
    ):
        BaseDocTemplate.__init__(self,
                                 filename,
                                 pagesize=pagesize,
                                 leftMargin=margin_left,
                                 rightMargin=margin_right,
                                 topMargin=margin_top,
                                 bottomMargin=margin_bottom,
                                 **kwargs)
        self.styles = styles
        self.total_pages = np.nan
        self.confidential = confidential

        self._cover = PdfCoverTemplate(self, logo_cover)
        self._content = PdfContentTemplate(self, logo_header, creation_date)

        self.addPageTemplates([self._cover, self._content])

    # noinspection PyUnresolvedReferences
    def afterFlowable(self, flowable):
        if flowable.__class__.__name__ == 'Paragraph':
            if flowable.style.name in ['Heading1', 'Heading2']:
                style = self.styles['TableOfContents']
                header = flowable.getPlainText()

                # TODO: Find better solution to mimic a tab for header numbers
                if '\xa0' in header:
                    header = [s for s in header.split('\xa0') if len(s) > 0]
                    while stringWidth(header[0], style.fontName, style.fontSize) < 10.5 * mm:
                        header[0] += '\xa0'
                    header = ''.join(header)

                entry = [0, header, self.page]

                # If there is a bookmark name, append that to the notified data
                bookmark = getattr(flowable, '_bookmarkName', None)
                if bookmark is not None:
                    entry.append(bookmark)
                self.notify('TOCEntry', tuple(entry))

    # noinspection PyPep8Naming
    def prepareBuild(self, *args, **kwargs):
        # TODO: Find better solution than building twice, to get total page number
        self.multiBuild(*args, **kwargs)
        self.total_pages = self.canv.getPageNumber() - 1


# noinspection PyUnresolvedReferences
class PdfPageTemplate(PageTemplate):

    # noinspection PyShadowingBuiltins
    def __init__(self, doc: PdfDocTemplate, id: str):
        super().__init__(frames=Frame(doc.leftMargin, doc.rightMargin, doc.width, doc.height, id=id.lower()),
                         onPage=self.draw, id=id)

        self.frame_left = doc.leftMargin
        self.frame_right = doc.leftMargin + doc.width
        self.frame_top = doc.bottomMargin + doc.height
        self.frame_bottom = doc.bottomMargin

    def draw(self, canvas: Canvas, doc: PdfDocTemplate):
        pass

    @staticmethod
    def _draw_image(canvas: Canvas, image_path: str, x: float, y: float, width: float = None, height: float = None):
        if str(image_path).endswith('.svg'):
            from reportlab.graphics import renderPDF
            from svglib.svglib import svg2rlg
            image = svg2rlg(image_path)

            if width is not None:
                scale = width / float(image.width)
            elif height is not None:
                scale = height / float(image.height)
            else:
                scale = 1
            image.width *= scale
            image.height *= scale
            image.scale(scale, scale)

            renderPDF.draw(image, canvas, x, y)
        else:
            image = ImageReader(image_path)
            image_width, image_height = image.getSize()

            if width is None:
                width = height * (float(image_width) / float(image_height))
            if height is None:
                height = width * (float(image_height) / float(image_width))

            canvas.drawImage(image, x, y, width=width, height=height, mask='auto')

    def _draw_header_overlay(self, canvas: Canvas, doc: PdfDocTemplate):
        # Add a white overlay, to reproduce the faded CD header
        header_width = doc.width + doc.leftMargin + doc.rightMargin
        header_height = doc.topMargin

        canvas.setFillColor(Color(1, 1, 1, alpha=0.5))
        canvas.rect(0, self.frame_top, header_width, header_height, fill=True, stroke=False)

    # noinspection PyMethodMayBeStatic
    def _draw_footer_overlay(self, canvas: Canvas, doc: PdfDocTemplate):
        # Add a white overlay, to reproduce the faded CD footer
        header_width = doc.width + doc.leftMargin + doc.rightMargin
        header_height = doc.bottomMargin

        canvas.setFillColor(Color(1, 1, 1, alpha=0.5))
        canvas.rect(0, 0, header_width, header_height, fill=True, stroke=False)


# noinspection PyUnresolvedReferences
class PdfCoverTemplate(PdfPageTemplate):

    def __init__(self, doc: PdfDocTemplate, logo_path: str):
        super().__init__(doc, id='Cover')
        self.logo_path = logo_path

    def draw(self, canvas: Canvas, doc: PdfDocTemplate):
        canvas.saveState()

        self._draw_image(canvas, self.logo_path, self.frame_left, self.frame_top, height=20 * mm)
        self._draw_header_overlay(canvas, doc)

        canvas.restoreState()


# noinspection PyUnresolvedReferences
class PdfContentTemplate(PdfPageTemplate):

    def __init__(self, doc: PdfDocTemplate, logo_path: str, creation_date: str):
        super().__init__(doc, id='Normal')
        self.logo_path = logo_path
        self.creation_date = creation_date

    def draw(self, canvas: Canvas, doc: PdfDocTemplate):
        self.draw_header(canvas, doc)
        self.draw_footer(canvas, doc)

    def draw_header(self, canvas: Canvas, doc: PdfDocTemplate):
        canvas.saveState()

        if doc.confidential:
            canvas.setFont(doc.styles.font_header, 11)
            canvas.drawCentredString(self.frame_left + doc.width/2, self.frame_top + 6 * mm,
                                     'CONFIDENTIAL', charSpace=3)

        canvas.drawRightString(self.frame_right, self.frame_top + 6 * mm, self.creation_date)

        self._draw_image(canvas, self.logo_path, self.frame_left, self.frame_top + 6 * mm, height=11 * mm)
        self._draw_header_overlay(canvas, doc)

        canvas.restoreState()

    def draw_footer(self, canvas: Canvas, doc: PdfDocTemplate):
        canvas.saveState()

        canvas.setStrokeColor("grey")
        canvas.line(self.frame_left, self.frame_bottom, self.frame_right, self.frame_bottom)

        canvas.setFont(doc.styles.font_header, 11)
        canvas.drawString(self.frame_left, self.frame_bottom - 6*mm, f'{canvas.getPageNumber()}/{doc.total_pages}')

        self._draw_footer_overlay(canvas, doc)

        canvas.restoreState()
