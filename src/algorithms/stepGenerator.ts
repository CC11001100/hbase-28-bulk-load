/**
 * BulkLoad 批量导入 — 步骤生成器
 *
 * 动画展示 BulkLoad 海量数据导入机制：通过 MapReduce 直接生成 HFile，
 * 按 Region 边界分区（TotalOrderPartitioner），再用 LoadIncrementalHFiles
 * 将 HFile 注册到 Region 的 Store（跳过 WAL/MemStore），实现 GB/s 级吞吐。
 * 对比普通 Put 写入走 WAL+MemStore 仅 MB/s。
 */
import type { Step, VisualElement, VariableState } from '../types'

/** BulkLoad 批量导入伪代码 */
export const TEMPLATE_CODE = `// BulkLoad：海量数据导入，MR 直接生成 HFile，绕过 WAL/MemStore

// 1. MR 作业生成 HFile，按 TotalOrderPartitioner 按 Region 边界分区
Job job = Job.getInstance(conf);
job.setMapperClass(BulkLoadMapper.class);     // 输出 ImmutableBytesWritable, KeyValue
job.setReducerClass(HFileReducer.class);     // 生成 HFile
job.setPartitionerClass(TotalOrderPartitioner.class); // 按 region 分区边界
HFileOutputFormat2.configureIncrementalLoad(job, table); // 输出 HFile

// 2. 执行 MR，生成 HFile 到 HDFS 临时目录
job.waitForCompletion(true);
Path hfileDir = new Path("/tmp/bulkload/hfiles");

// 3. bulk load：将 HFile 注册到 Region 的 Store，跳过 WAL
LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
loader.doBulkLoad(hfileDir, table);`

// 画布布局常量
const LAYOUT = {
  mr: { x: 60, y: 200, w: 150, h: 80, label: 'MapReduce 作业' },
  input: { x: 60, y: 50, w: 150, h: 60, label: '输入数据 (HDFS)' },
  partitioner: { x: 270, y: 200, w: 170, h: 80, label: 'TotalOrderPartitioner' },
  hfile: { x: 490, y: 200, w: 150, h: 80, label: 'HFile (HDFS)' },
  region: { x: 710, y: 200, w: 220, h: 80, label: 'Region / Store' },
  compare: { x: 490, y: 360, w: 220, h: 60, label: '吞吐对比' },
}

function makeElements(highlight?: string): VisualElement[] {
  const mk = (
    key: keyof typeof LAYOUT,
    type: string,
    state: string
  ): VisualElement => {
    const l = LAYOUT[key]
    return {
      id: key,
      type,
      label: l.label,
      x: l.x,
      y: l.y,
      width: l.w,
      height: l.h,
      state: key === highlight ? 'active' : state,
    }
  }
  return [
    mk('mr', 'job', 'idle'),
    mk('input', 'hdfs', 'idle'),
    mk('partitioner', 'partitioner', 'idle'),
    mk('hfile', 'hfile', 'idle'),
    mk('region', 'region', 'idle'),
    mk('compare', 'compare', 'idle'),
  ]
}

const BASE_VARS: VariableState[] = [
  { name: 'inputSize', value: '100GB', line: 4, type: 'long' },
  { name: 'viaWAL', value: 'false', line: 4, type: 'boolean' },
]

export function generateSteps(): Step[] {
  const steps: Step[] = []
  let idx = 0

  const push = (
    desc: string,
    line: number,
    vars: VariableState[],
    elements: VisualElement[],
    arrows: { from: string; to: string; label?: string }[] = [],
    actionLabel?: string,
    statusText?: string
  ) => {
    steps.push({
      index: idx++,
      description: desc,
      currentLine: line,
      variables: vars,
      elements,
      connections: arrows.map((a, i) => ({
        id: `arrow-${i}`,
        fromId: a.from,
        toId: a.to,
        kind: 'arrow' as const,
        label: a.label,
      })),
      annotations: [],
      actionLabel,
      statusText: statusText ?? desc,
    })
  }

  // 步骤 0：BulkLoad 总览
  push(
    'BulkLoad 海量导入：MR 直接生成 HFile，绕过 WAL/MemStore，实现高吞吐',
    4,
    [{ ...BASE_VARS[0] }],
    makeElements(),
    [
      { from: 'input', to: 'mr', label: '输入' },
      { from: 'mr', to: 'partitioner', label: '分区' },
      { from: 'partitioner', to: 'hfile', label: '生成 HFile' },
      { from: 'hfile', to: 'region', label: 'bulk load' },
    ],
    'OVERVIEW',
    'BulkLoad 总览'
  )

  // 步骤 1：输入数据
  push(
    '海量数据（100GB）位于 HDFS，作为 MR 作业输入',
    6,
    [{ name: 'inputSize', value: '100GB', line: 6, type: 'long' }],
    makeElements('input'),
    [{ from: 'input', to: 'mr', label: '1.读取 100GB' }],
    'INPUT',
    '读取输入数据'
  )

  // 步骤 2：MR 配置
  push(
    '配置 MR 作业：Mapper 输出 KeyValue，Reducer 生成 HFile',
    7,
    [
      { name: 'mapper', value: 'BulkLoadMapper', line: 7, type: 'Class' },
      { name: 'reducer', value: 'HFileReducer', line: 8, type: 'Class' },
    ],
    makeElements('mr'),
    [],
    'MR_SETUP',
    '配置 MR 作业'
  )

  // 步骤 3：TotalOrderPartitioner 分区
  push(
    'TotalOrderPartitioner 按 Region 边界分区：保证每个 HFile 对应一个 Region',
    10,
    [
      { name: 'partitioner', value: 'TotalOrderPartitioner', line: 10, type: 'Class' },
      { name: 'regionSplits', value: '[k1,k2,k3...]', line: 10, type: 'byte[][]' },
    ],
    makeElements('partitioner'),
    [{ from: 'mr', to: 'partitioner', label: '2.按 region 分区' }],
    'PARTITION',
    '按 Region 分区'
  )

  // 步骤 4：MR 生成 HFile
  push(
    'MR 执行完成：HFileOutputFormat2 直接输出 HFile 到 HDFS 临时目录',
    11,
    [
      { name: 'hfileCount', value: '20', line: 11, type: 'int' },
      { name: 'hfileDir', value: '/tmp/bulkload/hfiles', line: 14, type: 'Path' },
    ],
    makeElements('hfile'),
    [{ from: 'partitioner', to: 'hfile', label: '3.生成 20 个 HFile' }],
    'GEN_HFILE',
    '生成 HFile'
  )

  // 步骤 5：bulk load 挂载
  push(
    'doBulkLoad 将 HFile 注册到 Region 的 Store：直接挂载，跳过 WAL/MemStore',
    18,
    [
      { name: 'viaWAL', value: 'false', line: 18, type: 'boolean' },
      { name: 'loadAction', value: '挂载 HFile 到 Store', line: 18, type: 'String' },
    ],
    makeElements('region'),
    [{ from: 'hfile', to: 'region', label: '4.bulk load 挂载' }],
    'BULKLOAD',
    '挂载 HFile 到 Region'
  )

  // 步骤 6：吞吐对比
  push(
    '对比：普通 Put 走 WAL+MemStore 仅 MB/s；BulkLoad 直挂 HFile 达 GB/s',
    18,
    [
      { name: 'throughput', value: 'GB/s (BulkLoad)', line: 18, type: 'String' },
      { name: 'viaWAL', value: 'false', line: 18, type: 'boolean' },
    ],
    makeElements('compare'),
    [
      { from: 'hfile', to: 'compare', label: 'BulkLoad: GB/s' },
      { from: 'region', to: 'compare', label: '普通写: MB/s' },
    ],
    'COMPARE',
    '吞吐对比'
  )

  // 步骤 7：完成
  push(
    'BulkLoad 完成：100GB 数据绕过写路径直接落盘，无需 RegionServer 写入开销',
    19,
    [
      { name: 'inputSize', value: '100GB', line: 4, type: 'long' },
      { name: 'viaWAL', value: 'false', line: 19, type: 'boolean' },
      { name: 'throughput', value: 'GB/s', line: 19, type: 'String' },
    ],
    makeElements('region').map((e) => ({ ...e, state: 'done' })),
    [{ from: 'hfile', to: 'region', label: '已挂载' }],
    'DONE',
    'BulkLoad 完成'
  )

  return steps
}
