const TOPIC_TO_MAPPING_DATA = {
  // FORECAST VARIABLES
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
  precipitation: [
    {csvHeading: 'precipitation', jsonValueField: 'value', jsonUnitsField: 'units', influxValueField: 'precipitation', influxUnitsfield: 'precipitation_units', defaultUnits: 'in'},
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
};

module.exports = {
  TOPIC_TO_MAPPING_DATA,
};