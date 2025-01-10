package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.PROVISIONING_ROOT
import com.thetradedesk.audience.audienceVersionDateFormat

final case class AdvertiserRecord(
  AdvertiserId: String,
  IndustryCategoryId: BigInt
)

object SensitiveIndustryIds {
  val sensitive_industy_id: Seq[Int] = Seq(7162, //Medical Health
            7194, //Auto Buying and Selling
            7215, //Apprenticeships
            7216, //Career Advice
            7217, //Career Planning
            7219, //Remote Working
            7220, //Vocational Training
            7321, //Medical Tests
            7322, //Pharmaceutical Drugs
            7323, //Surgery
            7324, //Vaccines
            7325, //Cosmetic Medical Services
            7375, //Consumer Banking
            7377, //Financial Planning
            7402, //Apartments
            7407, //Houses
            7411, //Real Estate Buying and Selling
            7412, //Real Estate Renting and Leasing
            7787, //Human Resources
            7809, //Housing Market
            7810, //Interest Rates
            7811, //Job Market
            7844, //Job Fairs
            7845, //Resume Writing and Advice
            7890, //Allergies
            7891, //"Ear, // Nose and Throat Conditions"
            7893, //Eye and Vision Conditions
            7894, //Foot Health
            7895, //Heart and Cardiovascular Diseases
            7896, //Infectious Diseases
            7898, //Lung and Respiratory Health
            7899, //Mental Health
            7901, //Blood Disorders
            7903, //Skin and Dermatology
            7904, //Sleep Disorders
            7905, //Substance Abuse
            7906, //Bone and Joint Conditions
            7907, //Brain and Nervous System Disorders
            7908, //Cancer
            7909, //Cold and Flu
            7910, //Dental Health
            7911, //Diabetes
            7912, //Digestive Disorders
            7928, //Government Support and Welfare
            7929, //Student Financial Aid
            7931, //Home Insurance
            7936, //Credit Cards
            7937, //Home Financing
            7938, //Personal Loans
            7939, //Student Loans
            8287, //Bankruptcy
            8288, //Business Loans
            8289, //Debt Factoring & Invoice Discounting
            8298, //Hormonal Disorders
            8299, //Menopause
            8300, //Thyroid Disorders
            8301, //First Aid
            8302, //Birth Control
            8303, //Infertility
            8304, //Pregnancy
            8305, //Sexual Conditions
    )
}

case class AdvertiserDataset() 
  extends LightReadableDataset[AdvertiserRecord](
    "advertiser/v=1/",
    PROVISIONING_ROOT,
    dateFormat = audienceVersionDateFormat.substring(0, 8) // Extract the first 8 characters
  )
