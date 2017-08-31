/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { Pipe, PipeTransform } from '@angular/core';

const limit = 72;

@Pipe({
  name: 'centerEllipses'
})
export class CenterEllipsesPipe implements PipeTransform {
  private trail = '...';


  transform(value: any, length?: number): any {
      let tLimit = length ? length : limit;

      if (!value) {
        return '';
      }

      if (!length) {
        return value;
      }

      return value.length > tLimit
        ? value.substring(0, tLimit / 2) + this.trail + value.substring(value.length - tLimit / 2, value.length)
        : value;
  }

}