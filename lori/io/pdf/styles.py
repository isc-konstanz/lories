# -*- coding: utf-8 -*-
"""
lori.io.pdf.styles
~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations

import importlib.resources as resources

from reportlab.lib.enums import TA_CENTER
from reportlab.lib.colors import black
from reportlab.lib.styles import StyleSheet1, ParagraphStyle, ListStyle
from lori import Configurations

FONT_NORMAL: str = 'Calibri'
FONT_HEADER: str = 'Calibri-Light'


class PdfStyles(StyleSheet1):

    def __init__(self, configs: Configurations) -> None:
        super().__init__()
        from reportlab.pdfbase.pdfmetrics import registerFont
        from reportlab.pdfbase.ttfonts import TTFont

        font_res = resources.files('lori.io.font')

        registerFont(TTFont('Calibri', font_res.joinpath('Calibri.ttf')))
        registerFont(TTFont('Calibri-Bold', font_res.joinpath('Calibrib.ttf')))
        registerFont(TTFont('Calibri-Italic', font_res.joinpath('Calibrii.ttf')))
        registerFont(TTFont('Calibri-Bold-Italic', font_res.joinpath('Calibriz.ttf')))

        registerFont(TTFont('Calibri-Light', font_res.joinpath('Calibril.ttf')))
        registerFont(TTFont('Calibri-Light-Italic', font_res.joinpath('Calibrili.ttf')))

        fonts = configs.get_section("font", defaults={})
        self.font_normal = fonts.get("normal_type", default=FONT_NORMAL)
        self.font_header = fonts.get("header_type", default=FONT_HEADER)

        self.add(ParagraphStyle(name='Normal',
                                fontName=self.font_normal,
                                fontSize=11,
                                leading=12,
                                spaceBefore=6,
                                leftIndent=-6))  # TODO: Verify if this negative indent can be solved otherwise

        self.add(ParagraphStyle(name='Heading1',
                                parent=self['Normal'],
                                fontName=self.font_header,
                                fontSize=18,
                                leading=22,
                                spaceBefore=12,
                                spaceAfter=6), alias='h1')

        self.add(ParagraphStyle(name='Heading2',
                                parent=self['Heading1'],
                                fontSize=14,
                                leading=18,
                                spaceAfter=6), alias='h2')

        self.add(ParagraphStyle(name='Heading3',
                                parent=self['Heading1'],
                                fontName=f"{self.font_normal}-Bold",
                                fontSize=12,
                                leading=14,
                                spaceAfter=6,
                                textTransform='uppercase'), alias='h3')

        self.add(ParagraphStyle(name='Heading4',
                                parent=self['Heading1'],
                                fontName=f"{self.font_normal}-Bold-Italic",
                                fontSize=10,
                                leading=12,
                                spaceBefore=10,
                                spaceAfter=4), alias='h4')

        self.add(ParagraphStyle(name='Title',
                                parent=self['Heading1'],
                                fontSize=24,
                                leading=30,
                                alignment=TA_CENTER,
                                spaceAfter=6))

        self.add(ParagraphStyle(name='Subtitle',
                                parent=self['Heading1'],
                                fontSize=20,
                                leading=30,
                                alignment=TA_CENTER,
                                spaceAfter=6))

        self.add(ParagraphStyle(name='Cover',
                                parent=self['Normal'],
                                fontName=self.font_header,
                                fontSize=16,
                                alignment=TA_CENTER))

        self.add(ParagraphStyle(name='TableOfContents',
                                parent=self['Normal'],
                                spaceBefore=0))

        self.add(ListStyle(name='UnorderedList',
                           parent=None,
                           leftIndent=18,
                           rightIndent=0,
                           bulletAlign='left',
                           bulletType='1',
                           bulletColor=black,
                           bulletFontName=self.font_normal,
                           bulletFontSize=12,
                           bulletOffsetY=0,
                           bulletDedent='auto',
                           bulletDir='ltr',
                           bulletFormat=None,
                           # start='circle square blackstar sparkle disc diamond'.split(),
                           start=None))

        self.add(ListStyle(name='OrderedList',
                           parent=None,
                           leftIndent=18,
                           rightIndent=0,
                           bulletAlign='left',
                           bulletType='1',
                           bulletColor=black,
                           bulletFontName=self.font_normal,
                           bulletFontSize=12,
                           bulletOffsetY=0,
                           bulletDedent='auto',
                           bulletDir='ltr',
                           bulletFormat=None,
                           # start='1 a A i I'.split(),
                           start=None))
