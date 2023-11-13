<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:template match="/Файл">
    <data>
        <item><kind></kind></item>
        <xsl:variable name="total" select="count(Документ)"/>
        <xsl:variable name="file_id" select="@ИдФайл"/>
        <xsl:for-each select="Документ">
        <item>
            <kind>
                <xsl:value-of select="@ВидСубМСП"/>
            </kind>
            <category>
                <xsl:value-of select="@КатСубМСП"/>
            </category>
            <reestr_date>
                <xsl:value-of select="@ДатаВклМСП"/>
            </reestr_date>
            <data_date>
                <xsl:value-of select="@ДатаСост"/>
            </data_date>
            <ind_tin>
                <xsl:value-of select="ИПВклМСП/@ИННФЛ"/>
            </ind_tin>
            <ind_number>
                <xsl:value-of select="ИПВклМСП/@ОГРНИП"/>
            </ind_number>
            <first_name>
                <xsl:value-of select="ИПВклМСП/ФИОИП/@Имя"/>
            </first_name>
            <last_name>
                <xsl:value-of select="ИПВклМСП/ФИОИП/@Фамилия"/>
            </last_name>
            <patronymic>
                <xsl:value-of select="ИПВклМСП/ФИОИП/@Отчество"/>
            </patronymic>
            <org_name>
                <xsl:value-of select="ОргВклМСП/@НаимОрг"/>
            </org_name>
            <org_short_name>
                <xsl:value-of select="ОргВклМСП/@НаимОргСокр"/>
            </org_short_name>
            <org_tin>
                <xsl:value-of select="ОргВклМСП/@ИННЮЛ"/>
            </org_tin>
            <org_number>
                <xsl:value-of select="ОргВклМСП/@ОГРН"/>
            </org_number>
            <region_code>
                <xsl:value-of select="СведМН/@КодРегион"/>
            </region_code>
            <region_name>
                <xsl:value-of select="СведМН/Регион/@Наим"/>
            </region_name>
            <region_type>
                <xsl:value-of select="СведМН/Регион/@Тип"/>
            </region_type>
            <district_name>
                <xsl:value-of select="СведМН/Район/@Наим"/>
            </district_name>
            <district_type>
                <xsl:value-of select="СведМН/Район/@Тип"/>
            </district_type>
            <city_name>
                <xsl:value-of select="СведМН/Город/@Наим"/>
            </city_name>
            <city_type>
                <xsl:value-of select="СведМН/Город/@Тип"/>
            </city_type>
            <settlement_name>
                <xsl:value-of select="СведМН/НаселПункт/@Наим"/>
            </settlement_name>
            <settlement_type>
                <xsl:value-of select="СведМН/НаселПункт/@Тип"/>
            </settlement_type>
            <activity_code_main>
                <xsl:value-of select="СвОКВЭД/СвОКВЭДОсн/@КодОКВЭД"/>
            </activity_code_main>
            <activity_codes_additional>
                <xsl:for-each select="СвОКВЭД/СвОКВЭДДоп//@КодОКВЭД">
                    <xsl:value-of select="concat(., ', ')"/>
                </xsl:for-each>
            </activity_codes_additional>
            <total>
                <xsl:value-of select="$total"/>
            </total>
            <file_id>
                <xsl:value-of select="$file_id"/>
            </file_id>
        </item>
        </xsl:for-each>
    </data>
    </xsl:template>
</xsl:stylesheet>
