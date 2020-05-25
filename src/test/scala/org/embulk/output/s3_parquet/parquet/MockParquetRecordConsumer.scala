package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.io.api.{Binary, RecordConsumer}

case class MockParquetRecordConsumer() extends RecordConsumer {
  case class Data private (messages: Seq[Message] = Seq()) {
    def toData: Seq[Seq[Any]] = messages.map(_.toData)
  }
  case class Message private (fields: Seq[Field] = Seq()) {
    def toData: Seq[Any] = {
      val maxIndex: Int = fields.maxBy(_.index).index
      val raw: Map[Int, Any] = fields.map(f => f.index -> f.value).toMap
      0.to(maxIndex).map(idx => raw.get(idx).orNull)
    }
  }
  case class Field private (index: Int = 0, value: Any = null)

  private var _data: Data = Data()
  private var _message: Message = Message()
  private var _field: Field = Field()

  override def startMessage(): Unit = _message = Message()
  override def endMessage(): Unit =
    _data = _data.copy(messages = _data.messages :+ _message)
  override def startField(field: String, index: Int): Unit =
    _field = Field(index = index)
  override def endField(field: String, index: Int): Unit =
    _message = _message.copy(fields = _message.fields :+ _field)
  override def startGroup(): Unit = throw new UnsupportedOperationException
  override def endGroup(): Unit = throw new UnsupportedOperationException
  override def addInteger(value: Int): Unit =
    _field = _field.copy(value = value)
  override def addLong(value: Long): Unit = _field = _field.copy(value = value)
  override def addBoolean(value: Boolean): Unit =
    _field = _field.copy(value = value)
  override def addBinary(value: Binary): Unit =
    _field = _field.copy(value = value)
  override def addFloat(value: Float): Unit =
    _field = _field.copy(value = value)
  override def addDouble(value: Double): Unit =
    _field = _field.copy(value = value)

  def writingMessage(f: => Unit): Unit = {
    startMessage()
    f
    endMessage()
  }
  def writingField(field: String, index: Int)(f: => Unit): Unit = {
    startField(field, index)
    f
    endField(field, index)
  }
  def writingSampleField(f: => Unit): Unit = {
    writingMessage {
      writingField("a", 0)(f)
    }
  }
  def data: Seq[Seq[Any]] = _data.toData
}
