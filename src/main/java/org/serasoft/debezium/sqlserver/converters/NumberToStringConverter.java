/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.serasoft.debezium.sqlserver.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.Strings;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Properties;

public class NumberToStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

  private static final String FALLBACK = "";
  private static final String LANGUAGE_PROPERTY = "language";
  private static final String GROUPING_SEPARATOR_PROPERTY = "grouping.separator";
  private static final String INCLUDE_SIGN_PROPERTY = "include.sign";
  private static final String NEGATIVE_IN_PARENTHESIS_PROPERTY = "negatives.in.parentesis";
  private static final String PRECISION_PROPERTY = "precision";

  private Locale locale = Locale.getDefault();
  private String formatString = "%";

  private static final Logger LOGGER = LoggerFactory.getLogger(NumberToStringConverter.class);

  @Override
  public void configure(Properties properties) {

    final String language = properties.getProperty(LANGUAGE_PROPERTY);
    final String includeGrouping = properties.getProperty(GROUPING_SEPARATOR_PROPERTY);
    final String includeSign = properties.getProperty(INCLUDE_SIGN_PROPERTY);
    final String negativeInParenthesis = properties.getProperty(NEGATIVE_IN_PARENTHESIS_PROPERTY);
    final String precision = properties.getProperty(PRECISION_PROPERTY);

    if (!Strings.isNullOrEmpty(language)) {
      Locale selectedLocale = new Locale(language);
      if (!locale.equals(selectedLocale)) {
        LOGGER.info(
            "User required to change locale. Changing locale from "
                + locale
                + " to "
                + selectedLocale);
        locale = selectedLocale;
      }
    }
    if (!Strings.isNullOrEmpty(includeGrouping) && includeGrouping.equals("true")) {
      formatString += ",";
      LOGGER.info("User required to include grouping separator");
    }
    if (!Strings.isNullOrEmpty(includeSign) && includeSign.equals("true")) {
      formatString += "+";
      LOGGER.info("User required to always include a sign");
    }
    if (!Strings.isNullOrEmpty(negativeInParenthesis) && negativeInParenthesis.equals("true")) {
      formatString += "(";
      LOGGER.info("User required to include negative in parenthesis");
    }
    if (!Strings.isNullOrEmpty(precision) && Integer.parseInt(precision) > 0) {
      formatString += "." + precision;
      LOGGER.info("User required precision of " + precision);
    }
    formatString += "f";
  }

  @Override
  public void converterFor(
      RelationalColumn relationalColumn,
      ConverterRegistration<SchemaBuilder> converterRegistration) {

    if (!"decimal".equalsIgnoreCase(relationalColumn.typeName())
        && !"numeric".equalsIgnoreCase(relationalColumn.typeName())
        && !"money".equalsIgnoreCase(relationalColumn.typeName())
        && !"smallmoney".equalsIgnoreCase(relationalColumn.typeName())
    ) {
      return;
    }

    LOGGER.info(
        "Asked for convertion for field: "
            + relationalColumn.name()
            + " - Type: "
            + relationalColumn.typeName()
            + " - Locale: "
            + locale);

    converterRegistration.register(
        SchemaBuilder.string(),
        x -> {
          if (x == null) {
            if (relationalColumn.isOptional()) {
              return null;
            } else if (relationalColumn.hasDefaultValue()) {
              return relationalColumn.defaultValue();
            }
            return FALLBACK;
          }
          if (x instanceof Number || x instanceof BigDecimal) {

            String convertedValue = String.format(locale, formatString, x);
            LOGGER.info(
                "Converting field: "
                    + relationalColumn.name()
                    + "- Source value: "
                    + x
                    + " - Converted value: "
                    + convertedValue
                    + " - Locale: "
                    + locale);

            return convertedValue;
          }
          return FALLBACK;
        });
  }
}
