const TOPIC_TO_MAPPING_DATA = {
  // FORECAST VARIABLES, keys here are qmtt topics
  humidity: [
    {csvHeading: 'humidity', jsonValueField: 'converted-value', jsonUnitsField: 'converted-units', influxValueField: 'humidity', influxUnitsfield: 'humidity_units', defaultUnits: '%'},
  ],
  air_temperature: [
    {csvHeading: 'airtemp min', jsonValueField: 'min', jsonUnitsField: 'units', influxValueField: 'air_temp_min', influxUnitsfield: 'air_temp_units', defaultUnits: 'degC'},
    {csvHeading: 'airtemp max', jsonValueField: 'max', jsonUnitsField: 'units', influxValueField: 'air_temp_max', influxUnitsfield: 'air_temp_units', defaultUnits: 'degC'},
  ],
  wind_chill: [
    {csvHeading: 'wind chill', jsonValueField: 'value', jsonUnitsField: 'units', influxValueField: 'wind_chill', influxUnitsfield: 'wind_chill_units', defaultUnits: 'degC'},
  ],
  dewpoint: [
    {csvHeading: 'dew point', jsonValueField: 'value', jsonUnitsField: 'units', influxValueField: 'dewpoint_temperature', influxUnitsfield: 'dewpoint_units', defaultUnits: 'degC'},
  ],  
  ultraviolet: [
    {csvHeading: 'ultraviolet', jsonValueField: 'value', jsonUnitsField: 'units', influxValueField: 'ultraviolet', influxUnitsfield: 'ultraviolet_units', defaultUnits: 'W/m²'},
  ],    
  precipitation: [
    {csvHeading: 'precipitation', jsonValueField: 'value', jsonUnitsField: 'units', influxValueField: 'precipitation', influxUnitsfield: 'precipitation_units', defaultUnits: 'in'},
    {csvHeading: 'precipitation_cum', jsonValueField: 'acc', jsonUnitsField: 'units', influxValueField: 'precipitation_acc', influxUnitsfield: 'precipitation_units', defaultUnits: 'in'},
  ],
  wind_speed: [
    {csvHeading: 'wind speed max', jsonValueField: 'max', jsonUnitsField: 'speed_units', influxValueField: 'wind_speed_max', influxUnitsfield: 'wind_speed_units', defaultUnits: 'mph'},
    {csvHeading: 'wind speed avg', jsonValueField: 'avg', jsonUnitsField: 'speed_units', influxValueField: 'wind_speed_avg', influxUnitsfield: 'wind_speed_units', defaultUnits: 'mph'},
    {csvHeading: 'wind direction', jsonValueField: 'angle', jsonUnitsField: 'angle_units', influxValueField: 'wind_direction', influxUnitsfield: 'wind_direction_units', defaultUnits: 'deg'},
    {csvHeading: 'compass', jsonValueField: 'compass', jsonUnitsField: 'n/a', influxValueField: 'wind_direction_compass', influxUnitsfield: '', defaultUnits: 'n/a', isNonNumeric: true },
  ],
  soil_temperature: [
    {csvHeading: 'soiltemp 4in', jsonValueField: '_4in', jsonUnitsField: 'units', influxValueField: 'soil_temperature_4in', influxUnitsfield: 'soil_temperature_units', defaultUnits: 'degC'},
    {csvHeading: 'soiltemp 8in', jsonValueField: '_8in', jsonUnitsField: 'units', influxValueField: 'soil_temperature_8in', influxUnitsfield: 'soil_temperature_units', defaultUnits: 'degC'},
  ],
  insolation: [
    {csvHeading: 'insolation', jsonValueField: 'value', jsonUnitsField: 'units', influxValueField: 'insolation', influxUnitsfield: 'insolation_units', defaultUnits: 'Ly'},
  ],
  temperature: [
    {csvHeading: 'temperature', jsonValueField: 'converted-value', jsonUnitsField: 'converted-units', influxValueField: 'temperature', influxUnitsfield: 'temperature_units', defaultUnits: 'degC'},
  ],
  temperature3: [
    {csvHeading: 'temperature1', jsonValueField: 'temperature1', jsonUnitsField: 'units', influxValueField: 'temperature_1', influxUnitsfield: 'temperature_units', defaultUnits: 'degC'},
    {csvHeading: 'temperature2', jsonValueField: 'temperature2', jsonUnitsField: 'units', influxValueField: 'temperature_2', influxUnitsfield: 'temperature_units', defaultUnits: 'degC'},
    {csvHeading: 'temperature3', jsonValueField: 'temperature3', jsonUnitsField: 'units', influxValueField: 'temperature_3', influxUnitsfield: 'temperature_units', defaultUnits: 'degC'},
  ],
  pressure: [
    {csvHeading: 'pressure', jsonValueField: 'pressure', jsonUnitsField: 'pressure-units', influxValueField: 'pressure', influxUnitsfield: 'pressure_units', defaultUnits: 'Pa'},
  ],
  co2: [
    {csvHeading: 'co2', jsonValueField: 'compensated-value', jsonUnitsField: 'converted-units', influxValueField: 'co2', influxUnitsfield: 'co2_units', defaultUnits: 'ppm'},
  ],
  so2: [
    {csvHeading: 'so2', jsonValueField: 'compensated-value', jsonUnitsField: 'converted-units', influxValueField: 'co2', influxUnitsfield: 'so2_units', defaultUnits: 'ppb'},
  ],
  o3: [
    {csvHeading: 'o3', jsonValueField: 'compensated-value', jsonUnitsField: 'converted-units', influxValueField: 'co2', influxUnitsfield: 'o3_units', defaultUnits: 'ppb'},
  ],
  no2: [
    {csvHeading: 'no2', jsonValueField: 'compensated-value', jsonUnitsField: 'converted-units', influxValueField: 'no2', influxUnitsfield: 'no2_units', defaultUnits: 'ppb'},
  ],
  co: [
    {csvHeading: 'co', jsonValueField: 'compensated-value', jsonUnitsField: 'converted-units', influxValueField: 'co', influxUnitsfield: 'co_units', defaultUnits: 'ppm'},
  ],
  h2s: [
    {csvHeading: 'h2s', jsonValueField: 'compensated-value', jsonUnitsField: 'converted-units', influxValueField: 'h2s', influxUnitsfield: 'h2s_units', defaultUnits: 'ppb'},
  ],  
  exposure: [
    {csvHeading: 'exposure', jsonValueField: 'value', jsonUnitsField: '', influxValueField: 'exposure', influxUnitsfield: '', defaultUnits: '#'},
  ],
  full_particulate: [    
    {csvHeading: 'pm1.0', jsonValueField: 'pm1p0', jsonUnitsField: 'pm1p0_units', influxValueField: 'pm1p0', influxUnitsfield: 'pm1p0_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm2.5', jsonValueField: 'pm2p5', jsonUnitsField: 'pm2p5_units', influxValueField: 'pm2p5', influxUnitsfield: 'pm2p5_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm10.0', jsonValueField: 'pm10p0', jsonUnitsField: 'pm10p0_units', influxValueField: 'pm10p0', influxUnitsfield: 'pm10p0_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm1.0_cf1_a', jsonValueField: 'pm1p0_cf1_a', jsonUnitsField: 'units', influxValueField: 'pm1p0_cf1_a', influxUnitsfield: 'pm_cf1_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm2.5_cf1_a', jsonValueField: 'pm2p5_cf1_a', jsonUnitsField: 'units', influxValueField: 'pm2p5_cf1_a', influxUnitsfield: 'pm_cf1_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm10.0_cf1_a', jsonValueField: 'pm10p0_cf1_a', jsonUnitsField: 'units', influxValueField: 'pm10p0_cf1_a', influxUnitsfield: 'pm_cf1_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm1.0_atm_a', jsonValueField: 'pm1p0_atm_a', jsonUnitsField: 'units', influxValueField: 'pm1p0_atm_a', influxUnitsfield: 'pm_atm_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm2.5_atm_a', jsonValueField: 'pm2p5_atm_a', jsonUnitsField: 'units', influxValueField: 'pm2p5_atm_a', influxUnitsfield: 'pm_atm_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm10.0_atm_a', jsonValueField: 'pm10p0_atm_a', jsonUnitsField: 'units', influxValueField: 'pm10p0_atm_a', influxUnitsfield: 'pm_atm_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm0.3_cpl_a', jsonValueField: 'pm0p3_cpl_a', jsonUnitsField: 'units', influxValueField: 'pm0p3_cpl_a', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm0.5_cpl_a', jsonValueField: 'pm0p5_cpl_a', jsonUnitsField: 'units', influxValueField: 'pm0p5_cpl_a', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm1.0_cpl_a', jsonValueField: 'pm1p0_cpl_a', jsonUnitsField: 'units', influxValueField: 'pm1p0_cpl_a', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm2.5_cpl_a', jsonValueField: 'pm2p5_cpl_a', jsonUnitsField: 'units', influxValueField: 'pm2p5_cpl_a', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm5.0_cpl_a', jsonValueField: 'pm5p0_cpl_a', jsonUnitsField: 'units', influxValueField: 'pm5p0_cpl_a', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm10.0_cpl_a', jsonValueField: 'pm10p0_cpl_a', jsonUnitsField: 'units', influxValueField: 'pm10p0_cpl_a', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm1.0_cf1_b', jsonValueField: 'pm1p0_cf1_b', jsonUnitsField: 'units', influxValueField: 'pm1p0_cf1_b', influxUnitsfield: 'pm_cf1_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm2.5_cf1_b', jsonValueField: 'pm2p5_cf1_b', jsonUnitsField: 'units', influxValueField: 'pm2p5_cf1_b', influxUnitsfield: 'pm_cf1_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm10.0_cf1_b', jsonValueField: 'pm10p0_cf1_b', jsonUnitsField: 'units', influxValueField: 'pm10p0_cf1_b', influxUnitsfield: 'pm_cf1_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm1.0_atm_b', jsonValueField: 'pm1p0_atm_b', jsonUnitsField: 'units', influxValueField: 'pm1p0_atm_b', influxUnitsfield: 'pm_atm_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm2.5_atm_b', jsonValueField: 'pm2p5_atm_b', jsonUnitsField: 'units', influxValueField: 'pm2p5_atm_b', influxUnitsfield: 'pm_atm_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm10.0_atm_b', jsonValueField: 'pm10p0_atm_b', jsonUnitsField: 'units', influxValueField: 'pm10p0_atm_b', influxUnitsfield: 'pm_atm_units', defaultUnits: 'ug/m^3'},
    {csvHeading: 'pm0.3_cpl_b', jsonValueField: 'pm0p3_cpl_b', jsonUnitsField: 'units', influxValueField: 'pm0p3_cpl_b', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm0.5_cpl_b', jsonValueField: 'pm0p5_cpl_b', jsonUnitsField: 'units', influxValueField: 'pm0p5_cpl_b', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm1.0_cpl_b', jsonValueField: 'pm1p0_cpl_b', jsonUnitsField: 'units', influxValueField: 'pm1p0_cpl_b', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm2.5_cpl_b', jsonValueField: 'pm2p5_cpl_b', jsonUnitsField: 'units', influxValueField: 'pm2p5_cpl_b', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm5.0_cpl_b', jsonValueField: 'pm5p0_cpl_b', jsonUnitsField: 'units', influxValueField: 'pm5p0_cpl_b', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
    {csvHeading: 'pm10.0_cpl_b', jsonValueField: 'pm10p0_cpl_b', jsonUnitsField: 'units', influxValueField: 'pm10p0_cpl_b', influxUnitsfield: 'pm_cpl_units', defaultUnits: 'counts/L'},
  ],
  voc: [
    {csvHeading: 'voc', jsonValueField: 'compensated-tvoc', jsonUnitsField: 'tvoc-units', influxValueField: 'voc', influxUnitsfield: 'voc_units', defaultUnits: 'ppb'},
  ]
};

module.exports = {
  TOPIC_TO_MAPPING_DATA,
};