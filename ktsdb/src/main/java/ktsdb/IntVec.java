package ktsdb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A vector of primitive ints.
 *
 * The vector is backed by a contiguous array, and offers efficient random
 * access.
 */
@NotThreadSafe
final class IntVec {

  /** Default initial capacity for new vectors. */
  @VisibleForTesting
  static final int DEFAULT_CAPACITY = 32;

  /** data backing the vector. */
  private int[] data;

  /** offset of first unused element in data. */
  private int len;

  private IntVec(int capacity) {
    data = new int[capacity];
    len = 0;
  }

  private IntVec(int[] data) {
    this.data = data;
    this.len = data.length;
  }

  /**
   * Creates a new vector.
   * @return the new vector.
   */
  public static IntVec create() {
    return new IntVec(DEFAULT_CAPACITY);
  }

  /**
   * Creates a new vector with the specified capacity.
   * @param capacity the initial capacity of the vector
   * @return a new vector with the specified capacity
   */
  public static IntVec withCapacity(int capacity) {
    return new IntVec(capacity);
  }

  /**
   * Wrap an existing array with a vector.
   * The array should not be modified after this call.
   * @param data the initial data for the vector
   * @return a vector wrapping the data
   */
  public static IntVec wrap(int[] data) {
    return new IntVec(data);
  }

  /** Returns the number of elements the vector can hold without reallocating. */
  public int capacity() {
    return data.length;
  }

  /** Returns the number of elements in the vector. */
  public int len() {
    return len;
  }

  /** Returns {@code true} if the vector is empty. */
  public boolean isEmpty() {
    return len == 0;
  }

  /**
   * Reserves capacity for at least {@code additional} more elements to be
   * inserted into the vector.
   * The vector may reserve more space to avoid frequent reallocations. If the
   * vector already has sufficient capacity, no reallocation will happen.
   *
   * @param additional capacity to reserve
   */
  public void reserve(int additional) {
    if (additional < 0) throw new IllegalArgumentException("negative additional");
    if (data.length - len >= additional) return;
    reserveExact(Math.max(additional, (data.length - len) + data.length));
  }

  /**
   * Reserves capacity for exactly {@code additional} more elements to be
   * inserted into the vector.
   * The vector may reserve more space to avoid frequent reallocations. If the
   * vector already has sufficient capacity, no reallocation will happen.
   *
   * @param additional capacity to reserve
   */
  public void reserveExact(int additional) {
    if (len < 0) throw new IllegalArgumentException("negative additional");
    if (data.length - len > additional) return;
    data = Arrays.copyOf(data, data.length + additional);
  }

  /**
   * Shrink the capacity of the vector to match the length.
   */
  public void shrinkToFit() {
    if (len < data.length) data = Arrays.copyOf(data, len);
  }

  /**
   * Shorten the vector to be {@code len} elements long.
   * If {@code len} is greater than the vector's current length,
   * this has no effect.
   * @param len the new length of the vector
   */
  public void truncate(int len) {
    if (len < 0) throw new IllegalArgumentException("negative len");
    this.len = Math.min(this.len, len);
  }

  /**
   * Removes all elements from the vector.
   * No reallocation will be performed.
   */
  public void clear() {
    truncate(0);
  }

  /**
   * Appends an element to the vector.
   * @param element the element to append
   */
  public void push(int element) {
    reserve(1);
    data[len++] = element;
  }

  /**
   * Sets the element at {@code index} to the provided value.
   * @param index of the element to set
   * @param value to set the element to
   * @throws IndexOutOfBoundsException if {@code} index is not valid
   */
  public void set(int index, int value) {
    if (index >= len) throw new IndexOutOfBoundsException();
    data[index] = value;
  }

  /**
   * Concatenates another vector onto the end of this one.
   * @param other the other vector to concatenate onto this one
   */
  public void concat(IntVec other) {
    reserveExact(other.len);
    System.arraycopy(other.data, 0, data, len, other.len);
    len += other.len;
  }

  /**
   * Returns the element at the specified position.
   * @param index of the element to return
   * @return the element at the specified position
   * @throws IndexOutOfBoundsException if the index is out of range
   */
  public int get(int index) {
    if (index >= len) throw new IndexOutOfBoundsException();
    return data[index];
  }

  /**
   * Sorts the vector.
   */
  public void sort() {
    Arrays.sort(data, 0, len);
  }

  /**
   * Merges another vector into this one, retaining sort order.
   * Both vectors must initially be sorted. The other vector will not be
   * modified.
   * @param other the vector to merge into this vector
   */
  public void merge(IntVec other) {
    // http://www.programcreek.com/2012/12/leetcode-merge-sorted-array-java/
    reserve(other.len());

    int m = len;
    int n = other.len;

    while (m > 0 && n > 0) {
      if (data[m-1] > other.data[n-1]) {
        data[m+n-1] = data[m-1];
        m--;
      } else {
        data[m+n-1] = other.data[n-1];
        n--;
      }
    }

    while (n > 0) {
      data[m+n-1] = other.data[n-1];
      n--;
    }
    len += other.len;
  }

  /**
   * Removes all values from this vector that are not contained in the other
   * vector.
   * Both vectors should initially be sorted. This vector will remain sorted.
   * The other vector will not be modified. Duplicate values in both vectors
   * will be preserved.
   * @param other the vector to intersect with this vector
   */
  public void intersect(IntVec other) {
    int writeOffset = 0;
    int otherOffset = 0;

    for (int thisOffset = 0; thisOffset < len; thisOffset++) {
      int index = Arrays.binarySearch(other.data, otherOffset, other.len, data[thisOffset]);
      if (index < 0) {
        otherOffset = -index - 1;
      } else {
        data[writeOffset++] = other.data[index];
        otherOffset++; // prevent matching the element in the other vector again.
      }
    }
    this.len = writeOffset;
  }

  /**
   * Removes consecutive repeated elements in the vector.
   * If the vector is sorted, this removes all duplicates.
   */
  public void dedup() {
    if (len <= 1) return;

    int writeOffset = 1;
    for (int readOffset = 1; readOffset < len; readOffset++) {
      if (data[writeOffset - 1] != data[readOffset]) data[writeOffset++] = data[readOffset];
    }
    len = writeOffset;
  }

  /**
   * Creates an iterator over this vector.
   * The vector should not be concurrently modified while the iterator is in use.
   * @return an iterator over the vector
   */
  public Iterator iterator() {
    return new Iterator();
  }

  /**
   * Returns a list view of the vector.
   * The vector should not be concurrently modified while the list is in use.
   * @return a list view of the vector
   */
  public List<Integer> asList() {
    List<Integer> list = Ints.asList(data);
    if (len < data.length) return list.subList(0, len);
    return list;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    if (len == 0) {
      return "[]";
    }

    StringBuilder builder = new StringBuilder(len * 5);
    builder.append('[');
    builder.append(data[0]);
    for (int i = 1; i < len; i++) {
      builder.append(", ").append(data[i]);
    }
    builder.append(']');
    return builder.toString();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IntVec other = (IntVec) o;
    if (len != other.len) return false;
    for (int i = 0; i < len; i++) if (data[i] != other.data[i]) return false;
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int result = len;
    for (int i = 0; i < len; i++) result = 31 * result + data[i];
    return (int) result;
  }

  /** {@inheritDoc} */
  @Override
  protected IntVec clone() {
    IntVec clone = new IntVec(0);
    clone.data = Arrays.copyOf(data, data.length);
    clone.len = len;
    return clone;
  }

  /** An iterator of primitive ints. */
  public class Iterator {
    int index = 0;

    private Iterator() {}

    /**
     * Returns the next element in the iterator.
     * @return the next element
     */
    public int next() {
      return data[index++];
    }

    /**
     * Returns the next element in the iterator without changing the iterator's position.
     * @return the next element
     */
    public int peek() {
      return data[index];
    }

    /**
     * Returns {@code true} if the iterator contains another element.
     * @return {@code true} if the iterator has more elements
     */
    public boolean hasNext() {
      return index < len;
    }

    /**
     * Seeks this iterator to the provided index.
     * @param index the index to seek to
     * @throws IndexOutOfBoundsException if the index is out of bounds of the vector
     */
    public void seek(int index) {
      if (index < 0 || index > len) throw new IndexOutOfBoundsException("seek");
      this.index = index;
    }

    /**
     * Seek to the first datapoint greater than or equal to the provided value.
     * @param value to seek to
     */
    public void seekToValue(int value) {
      int offset = Arrays.binarySearch(data, value);
      index = offset >= 0 ? offset : -offset - 1;
    }

    /**
     * Get the iterator's current index in the vector.
     * @return the index
     */
    public int getIndex() {
      return index;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
        .add("index", index)
        .add("vec", IntVec.this)
        .toString();
    }
  }
}
