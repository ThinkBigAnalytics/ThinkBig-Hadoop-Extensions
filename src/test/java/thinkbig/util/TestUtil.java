/**
 * Copyright (C) 2010-2014 Think Big Analytics, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package thinkbig.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import junit.framework.TestCase;
import org.junit.Test;

public class TestUtil extends TestCase {

	/**
	 * Test to check if a date falls in between two other dates
	 */
	@Test
	public void testIsDateWithinDates() {
		try {
    		assertTrue(Util.isDateWithinDates(
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/08"),
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/07"),
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/09"))
    				);
    		assertFalse(Util.isDateWithinDates(
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/11"),
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/07"),
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/09"))
    				);
    		assertTrue(Util.isDateWithinDates(
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/07"),
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/07"),
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/09"))
    				);
    		assertTrue(Util.isDateWithinDates(
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/09"),
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/07"),
    				new SimpleDateFormat("yyyy/MM", Locale.ENGLISH).parse("2011/09"))
    				);
		} catch (ParseException e) {
			assertFalse(true);	
		}
	}

	/**
	 * Test to chekc if addDate works
	 */
	@Test
	public void testAddDates() {
		assertTrue(Util.addDays("2008-02-28", 1, "yyyy-MM-dd", "MM/yy/dd").equals("02/08/29"));
		assertTrue(Util.addDays("2008-02-28", -29, "yyyy-MM-dd", "MM/yy/dd").equals("01/08/30"));
		assertTrue(Util.addDays("2008-02-28", 0, "yyyy-MM-dd", "MM/yy/dd").equals("02/08/28"));
		assertTrue(Util.addDays("2008-12-31", 1, "yyyy-MM-dd", "MM/yyyy/dd").equals("01/2009/01"));
	}
}

