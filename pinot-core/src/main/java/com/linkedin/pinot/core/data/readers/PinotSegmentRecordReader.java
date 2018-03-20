/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.readers;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;


/**
 * Record reader for Pinot segment.
 */
public class PinotSegmentRecordReader implements RecordReader {
  private final IndexSegmentImpl _indexSegment;
  private final int _numDocs;
  private final Schema _schema;
  private final Map<String, PinotSegmentColumnReader> _columnReaderMap;

  private int _nextDocId = 0;
  private Integer[] _docIdsInSortedColumnOrder;

  /**
   * Read records using the segment schema.
   */
  public PinotSegmentRecordReader(File indexDir) throws Exception {
    _indexSegment = (IndexSegmentImpl) Loaders.IndexSegment.load(indexDir, ReadMode.mmap);
    try {
      SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) _indexSegment.getSegmentMetadata();
      _numDocs = segmentMetadata.getTotalRawDocs();
      _schema = segmentMetadata.getSchema();
      Collection<String> columnNames = _schema.getColumnNames();
      _columnReaderMap = new HashMap<>(columnNames.size());
      for (String columnName : columnNames) {
        _columnReaderMap.put(columnName, new PinotSegmentColumnReader(_indexSegment, columnName));
      }
    } catch (Exception e) {
      _indexSegment.destroy();
      throw e;
    }
  }

  /**
   * Read records using the passed in schema.
   * <p>Passed in schema must be a subset of the segment schema.
   */
  public PinotSegmentRecordReader(File indexDir, Schema schema) throws Exception {
    _indexSegment = (IndexSegmentImpl) Loaders.IndexSegment.load(indexDir, ReadMode.mmap);
    _schema = schema;
    try {
      SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) _indexSegment.getSegmentMetadata();
      _numDocs = segmentMetadata.getTotalRawDocs();
      Schema segmentSchema = segmentMetadata.getSchema();
      Collection<FieldSpec> fieldSpecs = _schema.getAllFieldSpecs();
      _columnReaderMap = new HashMap<>(fieldSpecs.size());
      for (FieldSpec fieldSpec : fieldSpecs) {
        String columnName = fieldSpec.getName();
        FieldSpec segmentFieldSpec = segmentSchema.getFieldSpecFor(columnName);
        Preconditions.checkState(fieldSpec.equals(segmentFieldSpec),
            "Field spec mismatch for column: %s, in the given schema: %s, in the segment schema: %s", columnName,
            fieldSpec, segmentFieldSpec);
        _columnReaderMap.put(columnName, new PinotSegmentColumnReader(_indexSegment, columnName));
      }
    } catch (Exception e) {
      _indexSegment.destroy();
      throw e;
    }
  }

  /**
   * Read records using the passed in schema and in the order of sorted column.
   */
  public PinotSegmentRecordReader(File indexDir, Schema schema, String sortedColumn) throws Exception {
    this(indexDir, schema);
    // If sorted column is not specified or the segment is already sorted, we don't need to compute ordering for docIds.
    if (sortedColumn != null && _schema.getFieldSpecFor(sortedColumn) != null) {
      PinotSegmentColumnReader columnReader = _columnReaderMap.get(sortedColumn);
      if (columnReader != null && !columnReader.isColumnSorted()) {
        _docIdsInSortedColumnOrder = getDocIdsInSortedColumnOrder(sortedColumn);
      }
    }
  }

  @Override
  public boolean hasNext() {
    return _nextDocId < _numDocs;
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    if (_docIdsInSortedColumnOrder == null) {
      reuse = getRecord(reuse, _nextDocId);
    } else {
      reuse = getRecord(reuse, _docIdsInSortedColumnOrder[_nextDocId]);
    }
    _nextDocId++;
    return reuse;
  }

  @Override
  public void rewind() {
    _nextDocId = 0;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close() {
    _indexSegment.destroy();
  }

  /**
   * Return the row given a docId
   */
  public GenericRow getRecord(GenericRow reuse, int docId) {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      if (fieldSpec.isSingleValueField()) {
        switch (fieldSpec.getDataType()) {
          case INT:
            reuse.putField(fieldName, _columnReaderMap.get(fieldName).readInt(docId));
            break;
          case LONG:
            reuse.putField(fieldName, _columnReaderMap.get(fieldName).readLong(docId));
            break;
          case FLOAT:
            reuse.putField(fieldName, _columnReaderMap.get(fieldName).readFloat(docId));
            break;
          case DOUBLE:
            reuse.putField(fieldName, _columnReaderMap.get(fieldName).readDouble(docId));
            break;
          case STRING:
            reuse.putField(fieldName, _columnReaderMap.get(fieldName).readString(docId));
            break;
          default:
            throw new IllegalStateException(
                "Field: " + fieldName + " has illegal data type: " + fieldSpec.getDataType());
        }
      } else {
        reuse.putField(fieldName, _columnReaderMap.get(fieldName).readMV(docId));
      }
    }
    return reuse;
  }

  /**
   * Get the array of docIds in the order of sorted values
   */
  public Integer[] getDocIdsInSortedColumnOrder(String sortedColumn) {
    FieldSpec fieldSpec = _schema.getFieldSpecFor(sortedColumn);
    if (fieldSpec.isSingleValueField()) {
      // Initialize the index array
      Integer[] sortedDocIds = new Integer[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        sortedDocIds[i] = i;
      }

      // Initialize record reader
      PinotSegmentColumnReader recordReader = _columnReaderMap.get(sortedColumn);

      // Fill in the comparator based on the data type
      ArrayIndexValueComparator comparator;
      switch (fieldSpec.getDataType()) {
        case INT:
          Integer[] intValues = new Integer[_numDocs];
          for (int i = 0; i < _numDocs; i++) {
            intValues[i] = (Integer) recordReader.readInt(i);
          }
          comparator = new ArrayIndexValueComparator(intValues, fieldSpec);
          break;
        case LONG:
          Long[] longValues = new Long[_numDocs];
          for (int i = 0; i < _numDocs; i++) {
            longValues[i] = (Long) recordReader.readLong(i);
          }
          comparator = new ArrayIndexValueComparator(longValues, fieldSpec);
          break;
        case FLOAT:
          Float[] floatValues = new Float[_numDocs];
          for (int i = 0; i < _numDocs; i++) {
            floatValues[i] = (Float) recordReader.readFloat(i);
          }
          comparator = new ArrayIndexValueComparator(floatValues, fieldSpec);
          break;
        case DOUBLE:
          Double[] doubleValues = new Double[_numDocs];
          for (int i = 0; i < _numDocs; i++) {
            doubleValues[i] = (Double) recordReader.readDouble(i);
          }
          comparator = new ArrayIndexValueComparator(doubleValues, fieldSpec);
          break;
        case STRING:
          String[] stringValues = new String[_numDocs];
          for (int i = 0; i < _numDocs; i++) {
            stringValues[i] = (String) recordReader.readString(i);
          }
          comparator = new ArrayIndexValueComparator(stringValues, fieldSpec);
          break;
        default:
          throw new IllegalStateException(
              "Field: " + sortedColumn + " has illegal data type: " + fieldSpec.getDataType());
      }
      Arrays.sort(sortedDocIds, comparator);
      return sortedDocIds;
    } else {
      throw new IllegalStateException("Sorted column is not supported for multi value column");
    }
  }

  /**
   * Comparator for sorting docIds in the order of sorted values.
   */
  public class ArrayIndexValueComparator implements Comparator<Integer> {
    private final Object[] _values;
    private final FieldSpec _fieldSpec;

    public ArrayIndexValueComparator(Object[] sortedValues, FieldSpec fieldSpec) {
      _values = sortedValues;
      _fieldSpec = fieldSpec;
    }

    @Override
    public int compare(Integer index1, Integer index2) {
      switch (_fieldSpec.getDataType()) {
        case INT:
          return ((Integer) _values[index1]).compareTo((Integer) _values[index2]);
        case LONG:
          return ((Long) _values[index1]).compareTo((Long) _values[index2]);
        case FLOAT:
          return ((Float) _values[index1]).compareTo((Float) _values[index2]);
        case DOUBLE:
          return ((Double) _values[index1]).compareTo((Double) _values[index2]);
        case STRING:
          return ((String) _values[index1]).compareTo((String) _values[index2]);
        default:
          throw new IllegalStateException(
              "Field: " + _fieldSpec.getName() + " has illegal data type: " + _fieldSpec.getDataType());
      }
    }
  }
}
