/*
regimen_1EGFR tyrosine kinase inhibitors (TKI) [Erlotinib, Gefitinib, Afatinib, Dacomitinib, Osimertinib]
regimen_2Other TKIs [Crizotinib, Ceritinib, Brigatinib, Alectinib, Lorlatinib, Entrectinib,, Capmatinib, Selpercatinib, Pralsetinib, Vandetanib, Cabozantinib, Lenvatinib, Larotrectinib, Dabrafenib+Trametinib]
regimen_3Immune checkpoint inhibitors (anti-PD1/L1, anti-CTLA-4 or both)
regimen_4Immune checkpoint inhibitors (anti-PD1/L1) and platinum doublet chemotherapy with or without anti-VEGF monoclonal antibody (mAb) or dual immune checkpoint inhibitors (anti-PD1 and anti-CTLA-4) and platinum doublet chemotherapy
regimen_5Platinum doublet chemotherapy with or without anti-VEGF mAb
regimen_6Single agent chemotherapy with or without anti-VEGF mAb [Pemetrexed with or without Bevacizumab, Docetaxel with or without Ramucirumab]*/
WITH cte AS (SELECT cohort_definition_id,
                    person_id,
                    Line_of_therapy,
                    regimen,
                    (case when regimen in ('erlotinib',
                                          'gefitinib',
                                          'afatinib',
                                          'dacomitinib',
                                          'osimertinib')
                            then 1 else 0 end) AS EGFR_tyrosine_kinase_inhibitors,

                    (case when regimen in ('crizotinib', 'ceritinib',
                              'brigatinib', 'alectinib',
                              'lorlatinib', 'entrectinib',
                              'capmatinib', 'selpercatinib',
                              'pralsetinib', 'vandetanib',
                              'cabozantinib', 'lenvatinib',
                              'larotrectinib') OR regimen like ('%dabrafenib%trametinib%')
                          then 1 else 0 end) AS Other_EGFR_tyrosine_kinase_inhibitors,

                   ( case when regimen in ('pembrolizumab', 'nivolumab', 'dostarlimab') then 1
                          else 0 end) AS Anti_PD_1,

                    (case  when regimen in ('atezolizumab', 'avelumab', 'durvalumab') then 1
                          else 0 end) AS Anti_L_1,

                    (case when regimen in ('ipilimumab') then 1 else 0 end) AS Anti_CTLA_4,

                    (case  when regimen in ('cisplatin', 'carboplatin') then 1
                          else 0 end) AS Platinum_doublet,

                    (case  when regimen in ('%pemetrexed%docetaxel%') then 1
                          else 0 end) AS Single_agent,

                    (case  when regimen in ('bevacizumab','ranibizumab','aflibercept','ramucirumab')
                          then 1 else 0 end) AS anti_VEGF_mAb

           FROM @cohortDatabaseSchema.@regimenStatsTable
           ORDER BY 1, 3, 2
           WHERE cohort_definition_id IN (@targetId))

SELECT cohort_definition_id,
       person_id,
       Line_of_therapy,

      (case when EGFR_tyrosine_kinase_inhibitors = 1
        then  'Reimen_1'

      when Other_EGFR_tyrosine_kinase_inhibitors = 1
        then  'Reimen_2'

      when Platinum_doublet = 1 AND Anti_PD_1 + Platinum_doublet +
      anti_VEGF_mAb >= 2 OR  Anti_L_1  + Platinum_doublet +
      anti_VEGF_mAb >= 2 OR Platinum_doublet + Anti_CTLA_4 +  anti_VEGF_mAb > 2
        then 'Regimen_4'

      when Platinum_doublet + anti_VEGF_mAb = 0 AND Anti_PD_1 + Anti_L_1
      + Anti_CTLA_4 >= 2
      then 'Regimen_3'

      when Platinum_doublet + anti_VEGF_mAb >= 1 AND Anti_PD_1 + Anti_L_1
      + Anti_CTLA_4 = 0 AND  Platinum_doublet = 1
      then 'Regimen_5'

      when Single_agent + anti_VEGF_mAb >= 1 AND Single_agent = 1
      then 'Regimen_6'

      else 'Other' end) AS Regimens_categories

FROM cte
ORDER BY 1, 3, 2, 4;