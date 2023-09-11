<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:template match="/Файл">
    <data>
        <xsl:variable name="file_id" select="@ИдФайл"/>
        <xsl:for-each select="Документ">
        <item>
            <org_tin>
                <xsl:value-of select="СведНП/@ИННЮЛ"/>
            </org_tin>
            <employees_count>
                <xsl:value-of select="СведССЧР/@КолРаб"/>
            </employees_count>
            <data_date>
                <xsl:value-of select="@ДатаСост"/>
            </data_date>
            <doc_date>
                <xsl:value-of select="@ДатаДок"/>
            </doc_date>
            <file_id>
                <xsl:value-of select="$file_id"/>
            </file_id>
        </item>
        </xsl:for-each>
    </data>
    </xsl:template>
</xsl:stylesheet>

