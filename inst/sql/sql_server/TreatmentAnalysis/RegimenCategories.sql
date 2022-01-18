/*
regimen_1EGFR tyrosine kinase inhibitors (TKI)
[Erlotinib, Gefitinib, Afatinib, Dacomitinib, Osimertinib]
regimen_2Other TKIs
[Crizotinib, Ceritinib, Brigatinib, Alectinib, Lorlatinib,
Entrectinib,, Capmatinib, Selpercatinib, Pralsetinib,
Vandetanib, Cabozantinib, Lenvatinib, Larotrectinib,
Dabrafenib+Trametinib]
regimen_3Immune checkpoint inhibitors
(anti-PD1/L1, anti-CTLA-4 or both)
regimen_4Immune checkpoint inhibitors
(anti-PD1/L1) and platinum doublet
chemotherapy with or without anti-VEGF
monoclonal antibody (mAb) or dual immune
checkpoint inhibitors (anti-PD1 and anti-CTLA-4)
and platinum doublet chemotherapy
regimen_5Platinum doublet chemotherapy
with or without anti-VEGF mAb
regimen_6Single agent chemotherapy with
or without anti-VEGF mAb [Pemetrexed
with or without Bevacizumab, Docetaxel
with or without Ramucirumab]
*/
WITH cte AS (SELECT cohort_definition_id,
                    person_id,
                    Line_of_therapy,
                    regimen,
                    (CASE WHEN regimen LIKE '%erlotinib%'OR
                     regimen LIKE '%gefitinib%'OR
                     regimen LIKE '%afatinib%'OR
                     regimen LIKE '%dacomitinib%'OR
                     regimen LIKE '%osimertinib%'

                            then 1 else 0 end) AS EGFR_tyrosine_kinase_inhibitors,

                    (CASE WHEN regimen LIKE
                    '%crizotinib%'OR
                    regimen LIKE '%ceritinib%'OR
                    regimen LIKE '%brigatinib%'OR
                    regimen LIKE '%alectinib%'OR
                    regimen LIKE '%lorlatinib%'OR
                    regimen LIKE '%entrectinib%'OR
                    regimen LIKE '%capmatinib%'OR
                    regimen LIKE '%selpercatinib%'OR
                    regimen LIKE '%pralsetinib%'OR
                    regimen LIKE '%vandetanib%'OR
                    regimen LIKE '%cabozantinib%'OR
                    regimen LIKE '%lenvatinib%'OR
                    regimen LIKE '%larotrectinib%'
                    OR regimen LIKE '%dabrafenib%trametinib%'

                          then 1 else 0 end) AS Other_EGFR_tyrosine_kinase_inhibitors,

                   ( CASE WHEN 
                   regimen LIKE '%pembrolizumab%' OR
                   regimen LIKE '%nivolumab%' OR
                   regimen LIKE '%dostarlimab%'
                    then 1
                          else 0 end) AS Anti_PD_1,

                    (CASE  WHEN regimen LIKE
                    '%atezolizumab%'OR
                    regimen LIKE '%avelumab%'OR
                    regimen LIKE '%durvalumab%'
                     then 1
                          else 0 end) AS Anti_L_1,

                    (CASE WHEN regimen LIKE
                    '%ipilimumab%'
                     then 1 else 0 end) AS Anti_CTLA_4,

                    (CASE  WHEN
                    regimen LIKE '%cisplatin%' OR
                    regimen LIKE '%carboplatin%'
                     then 1
                          else 0 end) AS Platinum_doublet,

                    (CASE  WHEN regimen LIKE
                    '%docetaxel%pemetrexed%'
                     then 1
                          else 0 end) AS Single_agent,

                    (CASE  WHEN regimen LIKE
                    '%bevacizumab%'OR
                    regimen LIKE '%ranibizumab%'OR
                    regimen LIKE '%aflibercept%'OR
                    regimen LIKE '%ramucirumab%'

                          then 1 else 0 end) AS anti_VEGF_mAb

           FROM @cohortDatabaseSchema.@regimenStatsTable
           WHERE cohort_definition_id IN (@targetIds)
           ORDER BY 1, 3, 2
)

SELECT cohort_definition_id,
       person_id,
       Line_of_therapy,

      (CASE WHEN EGFR_tyrosine_kinase_inhibitors = 1 AND
      Other_EGFR_tyrosine_kinase_inhibitors +
      Anti_PD_1 + Anti_L_1 + Platinum_doublet + Single_agent + anti_VEGF_mAb = 0
        then  'TKI'

      WHEN Other_EGFR_tyrosine_kinase_inhibitors = 1 AND EGFR_tyrosine_kinase_inhibitors +
      Anti_PD_1 + Anti_L_1 + Platinum_doublet + Single_agent + anti_VEGF_mAb = 0
        then  'Other_TKIs'

      WHEN Platinum_doublet = 1 AND Anti_PD_1 + Platinum_doublet +
      anti_VEGF_mAb >= 2 OR  Anti_L_1  + Platinum_doublet +
      anti_VEGF_mAb >= 2 OR Platinum_doublet + Anti_CTLA_4 +  anti_VEGF_mAb > 2
      AND Other_EGFR_tyrosine_kinase_inhibitors + EGFR_tyrosine_kinase_inhibitors +
      Single_agent = 0
        then 'anti-PD1/L1_and_Platinum_doublet' --!!!should present

      WHEN Platinum_doublet + anti_VEGF_mAb = 0 AND Anti_PD_1 + Anti_L_1
      + Anti_CTLA_4 >= 2 AND Other_EGFR_tyrosine_kinase_inhibitors +
      EGFR_tyrosine_kinase_inhibitors + Single_agent = 0
      then 'Immune_checkpoint_inhibitors' --!!!should present

      WHEN Platinum_doublet + anti_VEGF_mAb >= 1 AND Anti_PD_1 + Anti_L_1
      + Anti_CTLA_4 = 0 AND  Platinum_doublet = 1 AND Other_EGFR_tyrosine_kinase_inhibitors +
      EGFR_tyrosine_kinase_inhibitors + Single_agent = 0
      then 'Platinum_doublet'

      WHEN Single_agent + anti_VEGF_mAb >= 1 AND Single_agent = 1
      AND Other_EGFR_tyrosine_kinase_inhibitors +
      EGFR_tyrosine_kinase_inhibitors + Platinum_doublet + Anti_CTLA_4
      + Anti_PD_1 + Anti_L_1 = 0
      then 'Single_agent_chemotherapy'

      else 'Other' end) AS Regimens_categories

FROM cte
ORDER BY 1, 3, 2, 4;
