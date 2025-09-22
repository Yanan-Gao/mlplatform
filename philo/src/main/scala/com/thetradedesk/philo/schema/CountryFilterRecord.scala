package com.thetradedesk.philo.schema

/**
 * Schema for country filter data.
 * Used to filter datasets by specific countries.
 * 
 * @param Country country code or name to filter by
 */
case class CountryFilterRecord(Country: String)