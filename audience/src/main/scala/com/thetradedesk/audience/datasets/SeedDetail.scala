package com.thetradedesk.audience.datasets

final case class SeedDetail(
                             SeedId: String,
                             TargetingDataId: Long,
                             Count: Long,
                             Path: String
                           )

final case class SeedRecord(SeedId: String, UserId: String)
