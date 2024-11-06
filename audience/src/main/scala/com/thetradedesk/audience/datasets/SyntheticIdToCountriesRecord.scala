package com.thetradedesk.audience.datasets

final case class SyntheticIdToCountriesRecord(Country: Int,
                                              CountryToSyntheticIds: Seq[Int]
                                             )