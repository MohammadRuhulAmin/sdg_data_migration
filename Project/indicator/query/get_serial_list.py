query = """
        select sil.serial_no from sdg_indicator_langs sil
        where sil.language_id = 1
        -- and sil.serial_no = "1.3.1"
        group by sil.serial_no
        order by sil.serial_no;
        """