/*
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

package org.apache.flink.cep.nfa;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * 版本控制方案，允许检索不同版本之间的依赖关系。
 *
 * <p>Dewey编号由一系列数字 d1.d2.d3. ... .dn 组成。如果一个 Dewey 编号 v 与 v' 兼容，当且仅当 v 包含 v' 作为前缀，
 * 或者 v 和 v' 仅在最后一位数字上不同，并且 v 的最后一位数字大于 v'。
 */
public class DeweyNumber implements Serializable {

    private static final long serialVersionUID = 6170434818252267825L;

    // Dewey编号的数字序列
    private final int[] deweyNumber;

    /**
     * 构造函数，使用一个起始数字初始化 Dewey 编号。
     *
     * @param start Dewey 编号的起始数字
     */
    public DeweyNumber(int start) {
        deweyNumber = new int[] {start};
    }

    /**
     * 构造函数，复制另一个 Dewey 编号。
     *
     * @param number 另一个 Dewey 编号
     */
    public DeweyNumber(DeweyNumber number) {
        this.deweyNumber = Arrays.copyOf(number.deweyNumber, number.deweyNumber.length);
    }

    /**
     * 私有构造函数，使用一个整数数组初始化 Dewey 编号。
     *
     * @param deweyNumber Dewey 编号的数字数组
     */
    private DeweyNumber(int[] deweyNumber) {
        this.deweyNumber = deweyNumber;
    }

    /**
     * 检查当前 Dewey 编号是否与另一个 Dewey 编号兼容。
     *
     * <p>当且仅当当前 Dewey 编号包含另一个编号作为前缀，或者它们在最后一位数字上不同且当前编号的最后一位数字大于
     * 另一个编号的最后一位数字时，返回 true。
     *
     * @param other 另一个 Dewey 编号
     * @return 如果当前 Dewey 编号与另一个 Dewey 编号兼容，返回 true；否则返回 false
     */
    public boolean isCompatibleWith(DeweyNumber other) {
        if (length() > other.length()) {
            // 当前编号包含另一个编号作为前缀
            for (int i = 0; i < other.length(); i++) {
                if (other.deweyNumber[i] != deweyNumber[i]) {
                    return false; // 如果有不同的数字，返回 false
                }
            }

            return true;
        } else if (length() == other.length()) {
            // 当前编号和另一个编号长度相同，比较数字
            int lastIndex = length() - 1;
            for (int i = 0; i < lastIndex; i++) {
                if (other.deweyNumber[i] != deweyNumber[i]) {
                    return false; // 如果有不同的数字，返回 false
                }
            }

            // 检查最后一位数字是否更大或相等
            return deweyNumber[lastIndex] >= other.deweyNumber[lastIndex];
        } else {
            return false;
        }
    }

    /**
     * 获取 Dewey 编号的第一个数字。
     *
     * @return Dewey 编号的第一个数字
     */
    public int getRun() {
        return deweyNumber[0];
    }

    /**
     * 获取 Dewey 编号的长度（即数字的个数）。
     *
     * @return Dewey 编号的长度
     */
    public int length() {
        return deweyNumber.length;
    }

    /**
     * 创建一个新的 Dewey 编号，该编号的最后一位数字增加 1。
     *
     * @return 一个新的 Dewey 编号，最后一位数字加 1
     */
    public DeweyNumber increase() {
        return increase(1); // 默认增加1
    }

    /**
     * 创建一个新的 Dewey 编号，该编号的最后一位数字增加指定的次数。
     *
     * @param times 要增加的次数
     * @return 一个新的 Dewey 编号，最后一位数字增加指定次数
     */
    public DeweyNumber increase(int times) {
        int[] newDeweyNumber = Arrays.copyOf(deweyNumber, deweyNumber.length);
        newDeweyNumber[deweyNumber.length - 1] += times; // 增加最后一位数字

        return new DeweyNumber(newDeweyNumber); // 返回新生成的 Dewey 编号
    }

    /**
     * 创建一个新的 Dewey 编号，在当前编号的基础上添加一个新的0作为最后一位数字。
     *
     * @return 一个新的 Dewey 编号，其中当前编号作为前缀，最后一位数字是0
     */
    public DeweyNumber addStage() {
        int[] newDeweyNumber = Arrays.copyOf(deweyNumber, deweyNumber.length + 1); // 在现有编号末尾添加一个0

        return new DeweyNumber(newDeweyNumber); // 返回新的 Dewey 编号
    }

    /**
     * 重写 equals 方法，比较两个 Dewey 编号是否相等。
     *
     * @param obj 另一个对象
     * @return 如果两个 Dewey 编号相等，返回 true；否则返回 false
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DeweyNumber) {
            DeweyNumber other = (DeweyNumber) obj;

            return Arrays.equals(deweyNumber, other.deweyNumber); // 比较数字数组是否相等
        } else {
            return false;
        }
    }

    /**
     * 重写 hashCode 方法，生成 Dewey 编号的哈希值。
     *
     * @return Dewey 编号的哈希值
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(deweyNumber); // 计算数字数组的哈希值
    }

    /**
     * 重写 toString 方法，返回 Dewey 编号的字符串表示。
     *
     * @return Dewey 编号的字符串表示
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        // 将每个数字和"."连接起来，最后一个数字不加"."
        for (int i = 0; i < length() - 1; i++) {
            builder.append(deweyNumber[i]).append(".");
        }

        if (length() > 0) {
            builder.append(deweyNumber[length() - 1]);
        }

        return builder.toString(); // 返回最终字符串
    }

    /**
     * 从字符串表示创建 Dewey 编号。输入字符串必须是由点分隔的整数。
     *
     * @param deweyNumberString 用点分隔的整数字符串
     * @return 从输入字符串生成的 Dewey 编号
     */
    public static DeweyNumber fromString(final String deweyNumberString) {
        String[] splits = deweyNumberString.split("\\."); // 按"."分割字符串

        if (splits.length == 1) {
            return new DeweyNumber(Integer.parseInt(deweyNumberString)); // 如果只有一个数字，直接返回
        } else if (splits.length > 0) {
            int[] deweyNumber = new int[splits.length];

            for (int i = 0; i < splits.length; i++) {
                deweyNumber[i] = Integer.parseInt(splits[i]); // 将分割后的字符串转为整数
            }

            return new DeweyNumber(deweyNumber); // 返回生成的 Dewey 编号
        } else {
            throw new IllegalArgumentException(
                    "Failed to parse " + deweyNumberString + " as a Dewey number");
        }
    }

    /** Dewey 编号的序列化器，用于版本控制。 */
    public static class DeweyNumberSerializer extends TypeSerializerSingleton<DeweyNumber> {

        private static final long serialVersionUID = -5086792497034943656L;

        public static final DeweyNumberSerializer INSTANCE = new DeweyNumberSerializer();

        private DeweyNumberSerializer() {}

        @Override
        public boolean isImmutableType() {
            return false; // 该类型是可变的
        }

        @Override
        public DeweyNumber createInstance() {
            return new DeweyNumber(1); // 创建 Dewey 编号实例，初始为 1
        }

        @Override
        public DeweyNumber copy(DeweyNumber from) {
            return new DeweyNumber(from); // 复制 Dewey 编号
        }

        @Override
        public DeweyNumber copy(DeweyNumber from, DeweyNumber reuse) {
            return copy(from); // 使用复制方法
        }

        @Override
        public int getLength() {
            return -1; // 不支持固定长度
        }

        /**
         * 序列化方法，将 Dewey 编号序列化到目标输出视图。
         *
         * @param record Dewey 编号对象
         * @param target 序列化的目标输出视图
         * @throws IOException 序列化过程中可能抛出的异常
         */
        @Override
        public void serialize(DeweyNumber record, DataOutputView target) throws IOException {
            final int size = record.length();
            target.writeInt(size); // 写入 Dewey 编号的长度
            for (int i = 0; i < size; i++) {
                target.writeInt(record.deweyNumber[i]); // 写入每个数字
            }
        }

        /**
         * 反序列化方法，从输入视图中反序列化 Dewey 编号。
         *
         * @param source 输入数据视图
         * @return 反序列化后的 Dewey 编号
         * @throws IOException 反序列化过程中可能抛出的异常
         */
        @Override
        public DeweyNumber deserialize(DataInputView source) throws IOException {
            final int size = source.readInt(); // 读取 Dewey 编号的长度
            int[] number = new int[size];
            for (int i = 0; i < size; i++) {
                number[i] = source.readInt(); // 读取每个数字
            }
            return new DeweyNumber(number); // 返回反序列化的 Dewey 编号
        }

        @Override
        public DeweyNumber deserialize(DeweyNumber reuse, DataInputView source) throws IOException {
            return deserialize(source); // 使用反序列化方法
        }

        /**
         * 从源数据视图复制数据到目标数据视图。
         *
         * @param source 源数据视图
         * @param target 目标数据视图
         * @throws IOException 复制过程中可能抛出的异常
         */
        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            final int size = source.readInt(); // 读取长度
            target.writeInt(size); // 写入长度
            for (int i = 0; i < size; i++) {
                target.writeInt(source.readInt()); // 复制每个数字
            }
        }

        // -----------------------------------------------------------------------------------

        /**
         * 获取序列化器的配置快照，用于兼容性和格式演变。
         *
         * @return 配置快照
         */
        @Override
        public TypeSerializerSnapshot<DeweyNumber> snapshotConfiguration() {
            return new DeweyNumberSerializerSnapshot(); // 返回配置快照
        }

        /** Dewey 编号的序列化器配置快照，用于兼容性和格式演变 */
        @SuppressWarnings("WeakerAccess")
        public static final class DeweyNumberSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<DeweyNumber> {

            public DeweyNumberSerializerSnapshot() {
                super(() -> INSTANCE); // 使用实例创建快照
            }
        }
    }
}

