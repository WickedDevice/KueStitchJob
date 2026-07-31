const _ = require('lodash');
const moment = require('moment');

// bring egg types in
let eggTypes;
const possibleApiServerPaths = [
  '../aqeapiserver/routes/mock/egg-types.json', 
  '../AqeApiServer/routes/mock/egg-types.json',
]
for (const p of possibleApiServerPaths) {
  if (!eggTypes) {
    try {
      eggTypes = require(p);
    } catch (e) {
      //
    } 
  }
}
if (!Array.isArray(eggTypes)) {
  eggTypes = [];
}
const eggTypesByID = _.keyBy(eggTypes, v => v.id);


const UCUM = require('@lhncbc/ucum-lhc');
const ucum = UCUM.UcumLhcUtils.getInstance();

const { Parser } = require('expr-eval');
const exprParser = new Parser({
  operators: {
    logical: true,
    comparison: true,
    conditional: true,
    'in': false,
    assignment: false
  }
});
const compiledExpressionHash = {};
for (const t of eggTypes || []) {
  // pre-compile all the expressions that are known to exist up front
  for (const dS of t.derivedSensors || []) {
    if ((dS?.expr) && (!compiledExpressionHash[dS.expr])) {
      compiledExpressionHash[dS.expr] = exprParser.parse(dS.expr);
    }
  }
}


function calculateHeatIndexDegF(temperature , humidity , temperature_units = 'degC') {
  const tempF = temperature_units === 'degC' ?
    cToF(temperature) : temperature;
  const tmp = Math.round(tempF / 2) * 2;
  const hum = Math.round(humidity / 5) * 5;

  const ret= {
    category: 'safe',
    index: 0,
    value: Math.round((1.1 * tempF) - 10.3 + (0.047 * humidity))
    // ref: formula for below 80, https://brownmath.com/bsci/notheat.htm equation (8)
    //      https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml
  };

  if (tmp < 80) {
    return ret;
  }
  if (hum < 40) {
    return ret;
  }

  const humIdx = Math.round((hum - 40) / 5);
  const tmpIdx = Math.round((tmp - 80) / 2);
  const table = [
    [80, 81, 83, 85, 88, 91, 94, 97, 101, 105, 109, 114, 119, 124, 130, 136],
    [80, 82, 84, 87, 89, 93, 96, 100, 104, 109, 114, 119, 124, 130, 137],
    [81, 83, 85, 88, 91, 95, 99, 103, 108, 113, 118, 124, 131, 137],
    [81, 84, 86, 89, 93, 97, 101, 106, 112, 117, 124, 130, 137],
    [82, 84, 88, 91, 95, 100, 105, 110, 116, 123, 129, 137],
    [82, 85, 89, 93, 98, 103, 108, 114, 121, 128, 136],
    [83, 86, 90, 95, 100, 105, 112, 119, 126, 134],
    [84, 88, 92, 97, 103, 109, 116, 124, 132],
    [84, 89, 94, 100, 106, 113, 121, 129],
    [85, 90, 96, 102, 110, 117, 126, 135],
    [86, 91, 98, 105, 113, 122, 131],
    [86, 93, 100, 108, 117, 127],
    [87, 95, 103, 112, 121, 132]
  ];

  const index_table = [
    [1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4],
    [1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4],
    [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4],
    [1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4],
    [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4],
    [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4],
    [1, 1, 1, 2, 2, 3, 3, 3, 4, 4],
    [1, 1, 2, 2, 2, 3, 3, 3, 4],
    [1, 1, 2, 2, 3, 3, 3, 4],
    [1, 1, 2, 2, 3, 3, 4, 4],
    [1, 2, 2, 3, 3, 3, 4],
    [1, 2, 2, 3, 3, 4],
    [1, 2, 2, 3, 3, 4]
  ];

  const categoryMap = ['safe', 'caution', 'extreme caution', 'danger', 'extreme danger'];

  const first = table[humIdx]
  if (!first) {
    return undefined; // this is essentially impossible because humidity can't be > 100%
  }

  const second = first[tmpIdx];
  if (!second) {
    // temperature is above defined values for this humidity
    ret.value = first.slice(-1)[0];
    ret.greaterThan = true;
    ret.index = 4;
    ret.category = categoryMap[ret.index];
    return ret;
  }

  ret.value = second;
  ret.index = index_table[humIdx][tmpIdx];
  ret.category = categoryMap[ret.index];

  return ret; // return the heat index in degF
}

function calculateHeatIndexDegC(temperature, humidity , temperature_units = 'degC') {
  const ret = this.calculateHeatIndexDegF(temperature, humidity, temperature_units);
  if (!ret) return ret;
  ret.value = Math.round(fToC(ret.value));
  return ret;
}
function fToC(degF) {
  return (degF - 32.0) * 5.0 / 9.0;
}

function cToF(degC) {
  return (degC * (9.0 / 5.0)) + 32;
}

function isNumeric (n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

function ucumifyUnits(units) {
  const unitsMap = {
    'degC': 'Cel',
    'C': 'Cel',
    '°C': 'Cel',
    'degF': '[degF]',
    'F': '[degF]',
    '°F': '[degF]',
  };

  return unitsMap[units] || units;
}

function convertUnits(input, targetUnits) {
  const v = +input.v;
  targetUnits = ucumifyUnits(targetUnits);
  if (input.u && isNumeric(v)) {
    input.u = ucumifyUnits(input.u);
    try {
      const res = ucum.convertUnitTo(input.u, v, targetUnits);
      if (res.status === 'succeeded') {
        return res.toVal;
      } else {
        console.error(JSON.stringify(res, null, 2));
        return NaN;
      }
    } catch (e) {
      return NaN;
    }
  } else {
    return +input.v;
  }
}

function calculateDerivedValues(eggType, row) {
  // row: {[label: string]: number, [label_units: string]: string], ...}
  // e.g. {ac: 0, battery: 4.52, battery_units: "V", humidity: 66.34, humidity_units: "%", temperature: 23.02, temperature_units: "°C", time: "2026-07-27T07:31:53.000Z" }
  // augments the row object with the new information AND returns the new values as a {[label: string]: {t, v, u}} structure
  const ret = {};
  for(const dS of eggType?.derivedSensors || []) {
    let hasAllInputs = true;
    const datum = {};
    for (const input of dS.inputs || []) {
      if (!isNumeric(row[input.label])) {
        hasAllInputs = false;
        break;
      }
      
      if (!datum.timestamp) {
        datum.timestamp = moment(row.time, 'YYYY-MM-DDTHH:mm:ssZ');
      } else if (row.time){
        const m = moment(row.time, 'YYYY-MM-DDTHH:mm:ssZ');
        if (m.isAfter(datum.timestamp)) {
          datum.timestamp = m;
        }
      }

      if (input.requiredUnits) {
        datum[input.label] = convertUnits({v: +row[input.label], u: row[input.label + '_units']}, input.requiredUnits);
      } else {
        datum[input.label] = +row[input.label];
      }
    }                
    if (hasAllInputs) {
      const v = compiledExpressionHash[dS.expr].evaluate(datum);
      if (isNumeric(v)) {
        ret[dS.label] = {
          t: datum.timestamp.format('YYYY-MM-DDTHH:mm:ssZ'),
          v,
          u: dS.units
        };
        if (isNumeric(dS.decimalPlaces)) {
          ret[dS.label].v = +((ret[dS.label].v).toFixed(dS.decimalPlaces));
        }
        row[dS.label] = ret[dS.label].v;
        row[dS.label + '_units'] = ret[dS.label].u;        
      }
    }
  }

  return ret;
}

module.exports = {
  calculateHeatIndexDegC, calculateHeatIndexDegF, eggTypesByID, calculateDerivedValues, isNumeric, convertUnits, ucumifyUnits,
}