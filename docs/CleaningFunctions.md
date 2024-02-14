# Cleaning Functions
Client agnostic functions designed to standardise social media performance data received from Tracer.

For example, functions here can be used to clean column names or clean common types of data found in the Tracer data such
as Country the advert was placed in or the media type of the advert.

For example some of the adverts have "France" as the country, or "francais". These will get converted to FR by the extract_country_from_string() function

::: veetility.cleaning_functions